use crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::ServerWorkerState;

impl ServerWorkerState {
    pub(crate) fn effective_client_memory_limit(&self) -> Option<usize> {
        let configured = self.ctx.config().server.maxmemory_clients.trim();
        let limit = if let Some(percent) = configured.strip_suffix('%') {
            percent.parse::<usize>().ok().and_then(|value| {
                (self.ctx.config().server.maxmemory > 0)
                    .then_some(self.ctx.config().server.maxmemory as usize)
                    .and_then(|maxmemory| maxmemory.checked_mul(value))
                    .map(|bytes| bytes / 100)
            })
        } else {
            crate::runtime::server::worker::flows::maintenance::parse_client_memory_limit(
                configured,
            )
        };
        limit
            .filter(|value| *value > 0)
            .map(|value| value.max(128 * 1024))
    }

    fn projected_normal_client_memory(
        &self,
        connection_token: usize,
        pending_replies: &[Vec<u8>],
    ) -> usize {
        let connection =
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)];
        if matches!(
            connection.slot_migration_role,
            Some(SlotMigrationConnectionRole::Export(_))
        ) || self.is_master_link(connection.token().0)
            || self.is_replica_connection(connection.token().0)
        {
            0
        } else {
            connection
                .client_memory_usage_with_pending_replies(pending_replies)
                .total
        }
    }

    pub(crate) fn refresh_client_memory_usage(&mut self, token: Token, force: bool) {
        self.refresh_client_memory_usage_inner(token, force, &[]);
    }

    pub(crate) fn refresh_client_memory_usage_with_pending_replies(
        &mut self,
        token: Token,
        pending_replies: &[Vec<u8>],
    ) {
        self.refresh_client_memory_usage_inner(token, false, pending_replies);
    }

    fn refresh_client_memory_usage_inner(
        &mut self,
        token: Token,
        force: bool,
        pending_replies: &[Vec<u8>],
    ) {
        if !force && self.effective_client_memory_limit().is_none() {
            return;
        }
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0))
            .then_some(token.0)
        else {
            return;
        };
        if !self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_token))
        {
            return;
        }
        if !force
            && (self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .no_evict
                || self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .import_source)
        {
            return;
        }
        let current = self.projected_normal_client_memory(connection_token, pending_replies);
        let previous = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .replace_accounted_normal_memory(current);
        self.ctx.adjust_normal_client_memory(previous, current);
    }

    pub(crate) fn refresh_all_client_memory_usage(&mut self) {
        let tokens: Vec<Token> = self
            .connection_registry
            .iter()
            .map(|(_, connection)| connection.token())
            .collect();
        for token in tokens {
            self.refresh_client_memory_usage(token, true);
        }
    }

    fn refresh_evictable_client_memory_usage(&mut self) {
        let tokens: Vec<Token> = self
            .connection_registry
            .iter()
            .filter_map(|(_, connection)| {
                (!connection.no_evict && !connection.import_source).then_some(connection.token())
            })
            .collect();
        for token in tokens {
            self.refresh_client_memory_usage(token, true);
        }
    }

    pub(crate) fn apply_client_memory_limit_change(&mut self, was_enabled: bool) {
        if !was_enabled && self.effective_client_memory_limit().is_some() {
            self.refresh_evictable_client_memory_usage();
        }
    }

    pub(crate) fn remove_client_memory_contribution(&mut self, connection_token: usize) {
        let Some(connection) = self
            .connection_registry
            .get_mut(crate::framework::capabilities::io::Token(connection_token))
        else {
            return;
        };
        let previous = connection.take_accounted_normal_memory();
        self.ctx.adjust_normal_client_memory(previous, 0);
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::distributed::cluster::migration::{
        MigrationJobId, SlotMigrationConnectionRole,
    };
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::ServerWorkerState;

    fn insert_connection(runtime: &mut ServerWorkerState, token: Token) -> usize {
        let connection = Connection::new(token, ([127, 0, 0, 1], 6379).into(), false);

        runtime.connection_registry.insert(connection)
    }

    #[test]
    fn runtime_client_memory_refreshes_normal_and_excluded_contributions() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let token = Token(41);
        let connection_token = insert_connection(&mut runtime, token);
        let expected = runtime.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .client_memory_usage()
        .total;

        runtime.refresh_client_memory_usage(token, true);

        assert_eq!(runtime.ctx.normal_client_memory(), expected);
        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .accounted_normal_memory(),
            expected
        );

        let job_id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .expect("valid migration id");
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .slot_migration_role = Some(SlotMigrationConnectionRole::Export(job_id));
        runtime.refresh_client_memory_usage(token, true);

        assert_eq!(runtime.ctx.normal_client_memory(), 0);
        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .accounted_normal_memory(),
            0
        );
    }

    #[test]
    fn runtime_client_memory_removal_subtracts_cached_contribution_once() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let token = Token(42);
        let connection_token = insert_connection(&mut runtime, token);
        runtime.refresh_client_memory_usage(token, true);
        assert!(runtime.ctx.normal_client_memory() > 0);

        runtime.remove_client_memory_contribution(connection_token);
        runtime.remove_client_memory_contribution(connection_token);

        assert_eq!(runtime.ctx.normal_client_memory(), 0);
        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .accounted_normal_memory(),
            0
        );
    }

    #[test]
    fn enabling_client_eviction_refreshes_every_existing_contribution() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().server.maxmemory_clients = "0".to_string();
        let mut expected = 0usize;
        let mut connection_tokens = Vec::new();
        for token_value in 50..53 {
            let token = Token(token_value);
            let connection_token = insert_connection(&mut runtime, token);
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(vec![b'x'; 200 * 1024]);
            expected = expected.saturating_add(
                runtime.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .client_memory_usage()
                .total,
            );
            connection_tokens.push(connection_token);
        }
        assert!(connection_tokens
            .iter()
            .all(|connection_token| runtime.connection_registry
                [crate::framework::capabilities::io::Token(*connection_token)]
            .accounted_normal_memory()
                == 0));
        let was_enabled = runtime.effective_client_memory_limit().is_some();

        runtime.ctx.config_mut().server.maxmemory_clients = "1gb".to_string();
        runtime.apply_client_memory_limit_change(was_enabled);

        assert_eq!(runtime.ctx.normal_client_memory(), expected);
        assert!(connection_tokens
            .iter()
            .all(|connection_token| runtime.connection_registry
                [crate::framework::capabilities::io::Token(*connection_token)]
            .accounted_normal_memory()
                > 200 * 1024));
    }

    #[test]
    fn enabling_client_eviction_does_not_refresh_no_evict_client() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().server.maxmemory_clients = "0".to_string();
        let control_token = Token(60);
        let control_slab = insert_connection(&mut runtime, control_token);
        runtime.refresh_client_memory_usage(control_token, true);
        let stable_control_memory = runtime.connection_registry
            [crate::framework::capabilities::io::Token(control_slab)]
        .accounted_normal_memory();
        runtime.connection_registry[crate::framework::capabilities::io::Token(control_slab)]
            .no_evict = true;
        runtime.connection_registry[crate::framework::capabilities::io::Token(control_slab)]
            .queue_reply(vec![b'c'; 200 * 1024]);
        assert!(
            runtime.connection_registry[crate::framework::capabilities::io::Token(control_slab)]
                .client_memory_usage()
                .total
                > stable_control_memory
        );
        let data_token = Token(61);
        let data_slab = insert_connection(&mut runtime, data_token);
        runtime.connection_registry[crate::framework::capabilities::io::Token(data_slab)]
            .queue_reply(vec![b'd'; 200 * 1024]);
        let data_memory = runtime.connection_registry
            [crate::framework::capabilities::io::Token(data_slab)]
        .client_memory_usage()
        .total;
        let was_enabled = runtime.effective_client_memory_limit().is_some();

        runtime.ctx.config_mut().server.maxmemory_clients = "1gb".to_string();
        runtime.apply_client_memory_limit_change(was_enabled);
        runtime.refresh_client_memory_usage(control_token, false);

        assert_eq!(
            runtime.connection_registry[crate::framework::capabilities::io::Token(control_slab)]
                .accounted_normal_memory(),
            stable_control_memory
        );
        assert_eq!(
            runtime.connection_registry[crate::framework::capabilities::io::Token(data_slab)]
                .accounted_normal_memory(),
            data_memory
        );
        assert_eq!(
            runtime.ctx.normal_client_memory(),
            stable_control_memory + data_memory
        );
    }

    #[test]
    fn pending_dispatch_replies_update_only_the_current_cached_contribution() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().server.maxmemory_clients = "1gb".to_string();
        let token = Token(62);
        let connection_token = insert_connection(&mut runtime, token);
        runtime.refresh_client_memory_usage(token, true);
        let baseline = runtime.ctx.normal_client_memory();
        let replies = vec![vec![b'x'; 32 * 1024]];

        runtime.refresh_client_memory_usage_with_pending_replies(token, &replies);

        assert!(runtime.ctx.normal_client_memory() > baseline);
        assert_eq!(
            runtime.ctx.normal_client_memory(),
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .accounted_normal_memory()
        );
        assert_eq!(
            runtime.ctx.normal_client_memory(),
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .client_memory_usage_with_pending_replies(&replies)
            .total
        );
    }
}
