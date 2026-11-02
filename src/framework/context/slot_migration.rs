use super::ServerContext;
use crate::foundation::io::store::kv_engine::DbSet;

impl ServerContext {
    pub fn create_slot_import(
        &mut self,
        id: crate::engine::distributed::cluster::migration::MigrationJobId,
        ranges: &crate::engine::distributed::cluster::migration::SlotRanges,
    ) {
        if self.slot_imports.contains_key(&id) {
            return;
        }
        for range in ranges.iter() {
            for slot in range.start..=range.end {
                self.db.dbs.set_slot_importing(slot as usize, true);
            }
        }
        let dataset = crate::framework::distributed::migration::ImportDataset::new_like(
            self.dbs(),
            self.config(),
            ranges.clone(),
        );
        self.slot_import_infrastructure_memory = self
            .slot_import_infrastructure_memory
            .saturating_add(dataset.infrastructure_memory());
        self.slot_imports.insert(id, Box::new(dataset));
    }

    pub fn apply_slot_import(
        &mut self,
        id: &crate::engine::distributed::cluster::migration::MigrationJobId,
        args: crate::framework::CommandArgs,
        registry: &crate::framework::CommandRegistry,
    ) -> Result<(), crate::framework::distributed::migration::MigrationApplyError> {
        let Some(mut dataset) = self.slot_imports.remove(id) else {
            return Err(
                crate::framework::distributed::migration::MigrationApplyError::Command(
                    "missing import dataset".to_string(),
                ),
            );
        };
        let result = dataset.apply(args, registry);
        self.slot_imports.insert(id.clone(), dataset);
        result
    }

    pub(crate) fn apply_slot_import_with_execution(
        &mut self,
        id: &crate::engine::distributed::cluster::migration::MigrationJobId,
        args: crate::framework::CommandArgs,
        registry: &crate::framework::CommandRegistry,
    ) -> (
        Result<(), crate::framework::distributed::migration::MigrationApplyError>,
        crate::framework::aof_replay::ReplayExecution,
        u32,
    ) {
        let Some(mut dataset) = self.slot_imports.remove(id) else {
            return (
                Err(
                    crate::framework::distributed::migration::MigrationApplyError::Command(
                        "missing import dataset".to_string(),
                    ),
                ),
                crate::framework::aof_replay::ReplayExecution::NotInvoked,
                0,
            );
        };
        let result = dataset.apply_with_execution(args, registry);
        let selected_db = dataset.selected_db();
        self.slot_imports.insert(id.clone(), dataset);
        (result.0, result.1, selected_db)
    }

    pub fn absorb_serving_slots_into_import(
        &mut self,
        id: &crate::engine::distributed::cluster::migration::MigrationJobId,
        ranges: &crate::engine::distributed::cluster::migration::SlotRanges,
    ) {
        let Some(mut dataset) = self.slot_imports.remove(id) else {
            return;
        };
        for range in ranges.iter() {
            for slot in range.start..=range.end {
                let entries = self.take_slot_entries(slot as usize);
                dataset.dbs_mut().restore_slot_entries(entries);
                self.dbs_mut().set_slot_importing(slot as usize, true);
            }
        }
        self.slot_imports.insert(id.clone(), dataset);
    }

    pub fn slot_import_key_count(&self, db_id: u32, slot: usize) -> usize {
        self.slot_imports
            .values()
            .filter_map(|dataset| dataset.dbs().get(db_id))
            .map(|db| db.scan_slot_key_count(slot))
            .sum()
    }

    pub(crate) fn has_slot_import(
        &self,
        id: &crate::engine::distributed::cluster::migration::MigrationJobId,
    ) -> bool {
        self.slot_imports.contains_key(id)
    }

    pub fn slot_import_keys(&self, db_id: u32, slot: usize) -> Vec<Vec<u8>> {
        self.slot_imports
            .values()
            .filter_map(|dataset| dataset.dbs().get(db_id))
            .flat_map(|db| db.keys_in_slot(slot))
            .collect()
    }

    pub fn slot_import_snapshot(
        &self,
        id: &crate::engine::distributed::cluster::migration::MigrationJobId,
        ranges: &crate::engine::distributed::cluster::migration::SlotRanges,
    ) -> std::io::Result<Vec<u8>> {
        self.slot_imports
            .get(id)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?
            .command_snapshot(ranges)
    }

    pub(crate) fn slot_import_aof_snapshot(&self) -> std::io::Result<Vec<u8>> {
        let jobs = self
            .cluster_state
            .as_deref()
            .map(|cluster| cluster.migrations.views())
            .unwrap_or_default();
        let mut snapshot = Vec::new();
        for job in jobs.iter().filter(|job| {
            job.operation
                == crate::engine::distributed::cluster::migration::MigrationOperation::Import
                && !job.phase.is_terminal()
        }) {
            let mut fields = vec![
                b"CLUSTER".to_vec(),
                b"SYNCSLOTS".to_vec(),
                b"ESTABLISH".to_vec(),
                b"SOURCE".to_vec(),
                job.source.as_str().as_bytes().to_vec(),
                b"NAME".to_vec(),
                job.id.as_str().as_bytes().to_vec(),
                b"SLOTSRANGE".to_vec(),
            ];
            for range in job.ranges.iter() {
                fields.push(range.start.to_string().into_bytes());
                fields.push(range.end.to_string().into_bytes());
            }
            let refs = fields.iter().map(Vec::as_slice).collect::<Vec<_>>();
            snapshot.extend_from_slice(
                &crate::engine::distributed::replication::protocol::encode_command(&refs),
            );
            snapshot.extend_from_slice(&self.slot_import_snapshot(&job.id, &job.ranges)?);
        }
        Ok(snapshot)
    }

    pub fn commit_slot_import(
        &mut self,
        id: &crate::engine::distributed::cluster::migration::MigrationJobId,
        ranges: &crate::engine::distributed::cluster::migration::SlotRanges,
    ) -> Result<usize, crate::framework::distributed::migration::CommitImportError> {
        let Some(mut dataset) = self.slot_imports.remove(id) else {
            return Ok(0);
        };
        let result = dataset.commit_into(&mut self.db.dbs, ranges);
        if result.is_err() {
            self.slot_imports.insert(id.clone(), dataset);
        } else {
            self.slot_import_infrastructure_memory = self
                .slot_import_infrastructure_memory
                .saturating_sub(dataset.infrastructure_memory());
            for range in ranges.iter() {
                for slot in range.start..=range.end {
                    self.db.dbs.set_slot_importing(slot as usize, false);
                }
            }
        }
        result
    }

    pub fn discard_slot_import(
        &mut self,
        id: &crate::engine::distributed::cluster::migration::MigrationJobId,
    ) {
        let ranges = self
            .slot_imports
            .get(id)
            .map(|dataset| dataset.ranges().clone())
            .or_else(|| {
                self.cluster_state
                    .as_deref()
                    .and_then(|state| state.migrations.get(id))
                    .map(|job| job.ranges.clone())
            });
        if let Some(dataset) = self.slot_imports.remove(id) {
            self.slot_import_infrastructure_memory = self
                .slot_import_infrastructure_memory
                .saturating_sub(dataset.infrastructure_memory());
        }
        if let Some(ranges) = ranges {
            for range in ranges.iter() {
                for slot in range.start..=range.end {
                    self.db.dbs.set_slot_importing(slot as usize, false);
                }
            }
        }
    }

    pub(crate) fn clear_slot_imports(&mut self) {
        self.db.dbs.clear_importing_slots();
        self.slot_imports.clear();
        self.slot_import_infrastructure_memory = 0;
    }

    pub(crate) fn slot_import_dbs(&self) -> Vec<&DbSet> {
        self.slot_imports
            .values()
            .map(|dataset| dataset.dbs())
            .collect()
    }

    pub(crate) fn slot_import_dbs_with_ids(&self) -> Vec<(Vec<u8>, &DbSet)> {
        self.slot_imports
            .iter()
            .map(|(id, dataset)| (id.as_str().as_bytes().to_vec(), dataset.dbs()))
            .collect()
    }

    pub(crate) fn replace_slot_imports_from_rdb_aux(
        &mut self,
        aux_fields: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), String> {
        let metadata = aux_fields
            .iter()
            .filter(|(key, _)| {
                key.eq_ignore_ascii_case(
                    crate::framework::distributed::migration::SLOT_IMPORT_AUX_KEY,
                )
            })
            .map(|(_, value)| {
                crate::framework::distributed::migration::SlotImportMetadata::decode(value)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let config = self.config().clone();
        let mut imports = std::collections::HashMap::new();
        for import in &metadata {
            let before = crate::foundation::memory::util_alloc::current_thread_allocated();
            let mut dbs = self.dbs().empty_like();
            let infrastructure_memory =
                crate::foundation::memory::util_alloc::current_thread_allocated()
                    .saturating_sub(before);
            for range in import.ranges.iter() {
                for slot in range.start..=range.end {
                    let entries = self.take_slot_entries(slot as usize);
                    dbs.restore_slot_entries(entries);
                    self.dbs_mut().set_slot_importing(slot as usize, true);
                }
            }
            imports.insert(
                import.id.clone(),
                Box::new(
                    crate::framework::distributed::migration::ImportDataset::from_dbs(
                        dbs,
                        &config,
                        import.ranges.clone(),
                        infrastructure_memory,
                    ),
                ),
            );
        }

        if let Some(cluster) = self.cluster_state.as_deref_mut() {
            // A full-sync RDB replaces, rather than merges with, the replica's
            // transient migration runtime state. Keeping an older tracking
            // import makes the restored AUX job overlap with it and aborts the
            // replication link.
            cluster.clear_migrations_for_load();
            for import in metadata {
                let job = crate::engine::distributed::cluster::migration::MigrationJob::new_tracking_import(
                    import.id,
                    import.source,
                    import.target,
                    import.ranges,
                    crate::foundation::io::sys::time::unixtime(),
                );
                cluster
                    .migrations
                    .start(job)
                    .map_err(|error| format!("invalid slot import AUX: {error:?}"))?;
            }
        } else if !metadata.is_empty() {
            return Err("slot import AUX requires cluster mode".to_string());
        }
        self.slot_import_infrastructure_memory = imports
            .values()
            .map(|dataset| dataset.infrastructure_memory())
            .sum();
        self.slot_imports = imports;
        Ok(())
    }

    pub(crate) fn prepare_slot_migration_load(&mut self) {
        self.slot_imports.clear();
        self.slot_import_infrastructure_memory = 0;
        if let Some(cluster) = self.cluster_state.as_deref_mut() {
            cluster.clear_migrations_for_load();
        }
    }

    pub(crate) fn finish_slot_migration_load(&mut self) {
        let is_primary = self
            .cluster_state
            .as_deref()
            .is_some_and(|cluster| cluster.myself.is_primary());
        if !is_primary {
            return;
        }
        let ids = self
            .cluster_state
            .as_deref()
            .map(|cluster| {
                cluster
                    .migrations
                    .views()
                    .into_iter()
                    .filter(|job| {
                        !job.phase.is_terminal()
                            && job.operation
                                == crate::engine::distributed::cluster::migration::MigrationOperation::Import
                    })
                    .map(|job| job.id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for id in ids {
            if let Some(dataset) = self.slot_imports.remove(&id) {
                self.slot_import_infrastructure_memory = self
                    .slot_import_infrastructure_memory
                    .saturating_sub(dataset.infrastructure_memory());
            }
            if let Some(cluster) = self.cluster_state.as_deref_mut() {
                let _ = cluster.finish_migration(
                    &id,
                    crate::engine::distributed::cluster::migration::TerminalPhase::Failed,
                    Some("Process restarted".to_string()),
                    crate::foundation::io::sys::time::unixtime(),
                );
            }
        }
    }
}

impl ServerContext {
    /// Protect a client by its stable, externally visible client id.
    /// Protected clients are not freed during ACL LOAD or similar operations.
    /// The event loop handles the actual protection logic after command dispatch.
    pub fn protect_client(&mut self, id: u64) {
        self.protected_clients.push(id);
    }
}
