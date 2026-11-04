use crate::engine::command::CommandArgs;
use crate::engine::distributed::replication::{protocol, ReplicationOffset};
use crate::foundation::io::compression::{StreamDecoder, StreamKind, REPLICATION_BATCH_BYTES};
use crate::foundation::io::protocol::parser::Parser;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{
    ConnectionId, LinkError, MasterInfo, RdbLoadMode, RdbReceive, ReplicaAction, ReplicaEvent,
    ReplicaLinkState, ReplicaRdbChannelState, ReplicationApply,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionalPrimary {
    pub replid: String,
    pub end_offset: ReplicationOffset,
    pub db_id: u32,
    pub rdb_client_id: u64,
}

#[derive(Clone, Copy)]
enum RdbFramingState {
    Header,
    Body { remaining: usize },
    Eof { marker: [u8; 40] },
}

static NEXT_REPLICA_LINK_SESSION_ID: AtomicU64 = AtomicU64::new(1);

pub struct ReplicaLink {
    session_id: u64,
    connection_id: ConnectionId,
    master: MasterInfo,
    state: ReplicaLinkState,
    #[allow(dead_code)] // Wired into dual-channel negotiation in a later slice.
    rdb_channel_state: ReplicaRdbChannelState,
    rdb_transfer_failed: bool,
    dual_main_psync_paused: bool,
    input: Vec<u8>,
    compression_mode: String,
    compression_decoder: Option<StreamDecoder>,
    pending_wire_input_bytes: u64,
    main_wire_active: bool,
    rdb_wire_active: bool,
    pending_stream_decompression_error: bool,
    rdb_input: Vec<u8>,
    rdb_framing_state: Option<RdbFramingState>,
    rdb_eof_holdback: Vec<u8>,
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) provisional_primary: Option<ProvisionalPrimary>,
    /// Streaming full-sync RDB receive state, live only during the RDB phase.
    /// Framing forwards RDB-body bytes here; the backend supplies the DbSet and
    /// drives the paced decode (see `rdb_recv_path.rs`).
    rdb_receive: Option<RdbReceive>,
    rdb_snapshot_options:
        Option<crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions>,
    rdb_skip_checksums: bool,
    rdb_load_mode: Option<RdbLoadMode>,
    /// Trailing bytes withheld from the decoder during the diskless EOF-marker
    /// scan: any suffix that could be the start of a split marker. Never fed to
    /// the decoder until confirmed to precede the marker.
    eof_holdback: Vec<u8>,
    parser: Parser,
    selected_db: u32,
    master_replid: Option<String>,
    requested_offset: Option<ReplicationOffset>,
    received_offset: ReplicationOffset,
    decoded_offset: ReplicationOffset,
    applied_offset: ReplicationOffset,
    acked_offset: ReplicationOffset,
    ack_enabled_after_rdb_commit: bool,
    last_io_time_ms: i64,
    last_periodic_ack_ms: i64,
    deferred_events: VecDeque<ReplicaEvent>,
    primary_user: Option<Vec<u8>>,
    primary_auth: Option<Vec<u8>>,
    rdb_auth_pending: bool,
    dual_channel_enabled: bool,
    failover_request: bool,
    failover_accepted: bool,
    partial_resync_accepted: bool,
    full_resync_accepted: bool,
    pending_primary_replid_change: Option<String>,
    cluster_node_id: Option<String>,
}

impl ReplicaLink {
    pub fn new(connection_id: ConnectionId, master: MasterInfo) -> Self {
        let zero = ReplicationOffset::try_from(0).expect("zero is a valid replication offset");
        Self {
            session_id: NEXT_REPLICA_LINK_SESSION_ID.fetch_add(1, Ordering::Relaxed),
            connection_id,
            master,
            state: ReplicaLinkState::Connecting,
            rdb_channel_state: ReplicaRdbChannelState::None,
            rdb_transfer_failed: false,
            dual_main_psync_paused: false,
            input: Vec::new(),
            compression_mode: "no".to_string(),
            compression_decoder: None,
            pending_wire_input_bytes: 0,
            main_wire_active: false,
            rdb_wire_active: false,
            pending_stream_decompression_error: false,
            rdb_input: Vec::new(),
            rdb_framing_state: None,
            rdb_eof_holdback: Vec::new(),
            provisional_primary: None,
            rdb_receive: None,
            rdb_snapshot_options: None,
            rdb_skip_checksums: false,
            rdb_load_mode: None,
            eof_holdback: Vec::new(),
            parser: Parser::new(),
            selected_db: 0,
            master_replid: None,
            requested_offset: None,
            received_offset: zero,
            decoded_offset: zero,
            applied_offset: zero,
            acked_offset: zero,
            ack_enabled_after_rdb_commit: true,
            last_io_time_ms: crate::foundation::io::sys::time::mstime(),
            last_periodic_ack_ms: 0,
            deferred_events: VecDeque::new(),
            primary_user: None,
            primary_auth: None,
            rdb_auth_pending: false,
            dual_channel_enabled: true,
            failover_request: false,
            failover_accepted: false,
            partial_resync_accepted: false,
            full_resync_accepted: false,
            pending_primary_replid_change: None,
            cluster_node_id: None,
        }
    }

    #[cfg(test)]
    pub fn streaming(
        connection_id: ConnectionId,
        master: MasterInfo,
        replid: String,
        offset: ReplicationOffset,
    ) -> Self {
        let mut link = Self::new(connection_id, master);
        link.state = ReplicaLinkState::Streaming;
        link.master_replid = Some(replid);
        link.requested_offset = Some(offset);
        link.received_offset = offset;
        link.decoded_offset = offset;
        link.applied_offset = offset;
        link.acked_offset = offset;
        link
    }

    #[allow(dead_code)]
    pub fn provisional_primary(&self) -> Option<&ProvisionalPrimary> {
        self.provisional_primary.as_ref()
    }

    pub fn provisional_resume_position(&self) -> Option<(&str, ReplicationOffset, u32)> {
        self.provisional_primary
            .as_ref()
            .map(|primary| (primary.replid.as_str(), primary.end_offset, primary.db_id))
    }

    pub fn clear_provisional_primary(&mut self) {
        self.provisional_primary = None;
    }

    pub fn connected(
        &mut self,
        primary_user: Option<&[u8]>,
        primary_auth: Option<&[u8]>,
    ) -> Vec<ReplicaAction> {
        self.primary_user = primary_user.map(ToOwned::to_owned);
        self.primary_auth = primary_auth.map(ToOwned::to_owned);
        if let Some(password) = primary_auth {
            self.state = ReplicaLinkState::WaitAuthReply;
            vec![ReplicaAction::Send(protocol::build_auth(
                primary_user,
                password,
            ))]
        } else {
            self.state = ReplicaLinkState::WaitPong;
            vec![ReplicaAction::Send(protocol::build_ping())]
        }
    }

    pub fn set_resume_position(
        &mut self,
        replid: String,
        offset: ReplicationOffset,
        selected_db: u32,
    ) {
        self.master_replid = Some(replid);
        self.requested_offset = Some(offset);
        self.received_offset = offset;
        self.decoded_offset = offset;
        self.applied_offset = offset;
        self.acked_offset = offset;
        // A partial resync replays the backlog tail as raw bytes; if the last
        // SELECT preceded `offset`, that tail arrives without a leading SELECT,
        // so resume on the DB the previous link had reached (§5.6).
        self.selected_db = selected_db;
    }

    pub fn set_dual_channel_enabled(&mut self, enabled: bool) {
        self.dual_channel_enabled = enabled;
    }

    pub fn set_compression_mode(&mut self, mode: &str) {
        self.compression_mode = mode.to_string();
        self.compression_decoder =
            (mode != "no").then(|| StreamDecoder::new(StreamKind::Replication, false));
    }

    pub fn advertised_compression_mode(&self) -> &str {
        &self.compression_mode
    }

    pub fn compression_is_plaintext(&self) -> bool {
        self.compression_decoder
            .as_ref()
            .is_some_and(StreamDecoder::is_plaintext)
    }

    pub fn take_wire_input_bytes(&mut self) -> u64 {
        std::mem::take(&mut self.pending_wire_input_bytes)
    }

    pub fn has_stream_decompression_error(&self) -> bool {
        self.pending_stream_decompression_error
    }

    pub fn prepare_rdb_snapshot_options(
        &mut self,
        options: crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions,
        skip_checksums: bool,
    ) {
        if self.rdb_receive.is_none() {
            self.rdb_snapshot_options = Some(options);
            self.rdb_skip_checksums = skip_checksums;
        }
    }

    pub fn has_pending_decompression(&self) -> bool {
        self.has_pending_main_decompression()
            || self
                .rdb_receive
                .as_ref()
                .is_some_and(RdbReceive::has_pending_decompression)
    }

    /// Pending command-stream decoding belongs to the main connection.
    /// The independent dual-channel RDB receiver cannot make it readable.
    pub fn has_pending_main_decompression(&self) -> bool {
        self.compression_decoder
            .as_ref()
            .is_some_and(StreamDecoder::has_pending_decode)
    }

    pub fn compression_memory_bytes(&self) -> usize {
        self.compression_decoder
            .as_ref()
            .map_or(0, StreamDecoder::memory_bytes)
            + self
                .rdb_receive
                .as_ref()
                .map_or(0, RdbReceive::compression_memory_bytes)
    }

    pub fn set_failover_request(&mut self, enabled: bool) {
        self.failover_request = enabled;
    }

    pub fn take_failover_accepted(&mut self) -> bool {
        std::mem::take(&mut self.failover_accepted)
    }

    pub fn take_partial_resync_accepted(&mut self) -> bool {
        std::mem::take(&mut self.partial_resync_accepted)
    }

    pub fn take_full_resync_accepted(&mut self) -> bool {
        std::mem::take(&mut self.full_resync_accepted)
    }

    fn mark_failover_accepted(&mut self) {
        self.failover_accepted = self.failover_request;
        self.failover_request = false;
    }

    pub fn set_cluster_node_id(&mut self, node_id: Option<String>) {
        self.cluster_node_id = node_id;
    }

    #[cfg(test)]
    /// Advances the replica protocol state with newly received bytes.
    ///
    /// Once the link is streaming, an error is terminal: parsing may have
    /// consumed internal input before rejecting the frame. The caller must
    /// discard this link and tear down its transport instead of retrying it.
    pub fn on_bytes(
        &mut self,
        bytes: &[u8],
        listening_port: u16,
    ) -> Result<(Vec<ReplicaEvent>, Vec<ReplicaAction>), LinkError> {
        self.on_bytes_with_announce_ip(bytes, listening_port, "")
    }

    /// Equivalent to [`Self::on_bytes`] with an explicit announced address.
    /// A streaming error has the same terminal discard-and-teardown contract.
    pub fn on_bytes_with_announce_ip(
        &mut self,
        bytes: &[u8],
        listening_port: u16,
        announce_ip: &str,
    ) -> Result<(Vec<ReplicaEvent>, Vec<ReplicaAction>), LinkError> {
        self.on_bytes_with_announce_ip_policy(bytes, listening_port, announce_ip, false)
    }

    pub(crate) fn on_bytes_while_script_blocked(
        &mut self,
        bytes: &[u8],
        listening_port: u16,
        announce_ip: &str,
    ) -> Result<(Vec<ReplicaEvent>, Vec<ReplicaAction>), LinkError> {
        self.on_bytes_with_announce_ip_policy(bytes, listening_port, announce_ip, true)
    }

    fn on_bytes_with_announce_ip_policy(
        &mut self,
        bytes: &[u8],
        listening_port: u16,
        announce_ip: &str,
        defer_rdb: bool,
    ) -> Result<(Vec<ReplicaEvent>, Vec<ReplicaAction>), LinkError> {
        if self.main_wire_active {
            self.pending_wire_input_bytes += bytes.len() as u64;
        }
        let prevalidated_streaming_received_offset =
            if self.is_streaming() && self.compression_decoder.is_none() {
                let input_len = self
                    .input
                    .len()
                    .checked_add(bytes.len())
                    .ok_or(LinkError::Protocol)?;
                Some(self.prevalidate_streaming_input(input_len)?)
            } else {
                None
            };
        self.last_io_time_ms = crate::foundation::io::sys::time::mstime();
        self.input.extend_from_slice(bytes);
        let mut events = Vec::new();
        let mut actions = Vec::new();

        loop {
            // C's replicaReceiveRDBFromPrimaryToMemory() leaves the RDB bytes
            // untouched while a yielding long command is active. Handshake
            // replies may still advance to this boundary, but dataset loading
            // must wait until the script/function has unwound.
            if defer_rdb && self.is_receiving_rdb() {
                break;
            }
            match self.state {
                ReplicaLinkState::Connecting => break,
                ReplicaLinkState::WaitAuthReply => {
                    let Some(line) = take_line(&mut self.input) else {
                        break;
                    };
                    if !line.starts_with(b"+") {
                        return Err(LinkError::Authentication);
                    }
                    self.state = ReplicaLinkState::WaitPong;
                    actions.push(ReplicaAction::Send(protocol::build_ping()));
                }
                ReplicaLinkState::WaitPong => {
                    let Some(line) = take_line(&mut self.input) else {
                        break;
                    };
                    if !line.starts_with(b"+") {
                        return Err(LinkError::Protocol);
                    }
                    // Pipeline REPLCONF port [, IP], capa together; then expect
                    // N +OK replies before advancing to PSYNC.
                    actions.push(ReplicaAction::Send(protocol::build_replconf_port(
                        listening_port,
                    )));
                    if !announce_ip.is_empty() {
                        actions.push(ReplicaAction::Send(protocol::build_replconf_ip_address(
                            announce_ip,
                        )));
                    }
                    actions.push(ReplicaAction::Send(protocol::build_replconf_capabilities(
                        self.dual_channel_enabled,
                        &self.compression_mode,
                    )));
                    if let Some(node_id) = &self.cluster_node_id {
                        actions.push(ReplicaAction::Send(protocol::build_replconf_version()));
                        actions.push(ReplicaAction::Send(
                            protocol::build_replconf_cluster_node_id(node_id),
                        ));
                    }
                    let remaining = 2
                        + usize::from(!announce_ip.is_empty())
                        + usize::from(self.cluster_node_id.is_some()) * 2;
                    self.state = ReplicaLinkState::WaitReplconfReplies {
                        remaining: remaining as u8,
                    };
                }
                ReplicaLinkState::WaitReplconfReplies { remaining } => {
                    let Some(line) = take_line(&mut self.input) else {
                        break;
                    };
                    if !line.starts_with(b"+") {
                        return Err(LinkError::Protocol);
                    }
                    match remaining.checked_sub(1) {
                        Some(0) => {
                            let requested = self
                                .requested_offset
                                .map(|offset| {
                                    offset.checked_next_request().ok_or(LinkError::Protocol)
                                })
                                .transpose()?;
                            let psync = match (&self.master_replid, requested) {
                                (Some(replid), Some(requested)) if self.failover_request => {
                                    protocol::build_psync_failover(replid, requested.get())
                                }
                                (Some(replid), Some(requested)) => {
                                    protocol::build_psync(replid, requested.get())
                                }
                                _ if self.failover_request => {
                                    protocol::build_psync_failover("?", -1)
                                }
                                _ => protocol::build_psync_full(),
                            };
                            actions.push(ReplicaAction::Send(psync));
                            self.state = ReplicaLinkState::WaitPsyncReply;
                        }
                        Some(n) => {
                            self.state = ReplicaLinkState::WaitReplconfReplies { remaining: n };
                        }
                        None => return Err(LinkError::Protocol),
                    }
                }
                ReplicaLinkState::WaitDualRdbClientIdReply => {
                    let Some(line) = take_line(&mut self.input) else {
                        break;
                    };
                    crate::server_log!(
                        debug,
                        "dual-rdb main handshake: state=WaitDualRdbClientIdReply reply={:?}",
                        String::from_utf8_lossy(&line)
                    );
                    if !line.starts_with(b"+") {
                        return Err(LinkError::Protocol);
                    }
                    let provisional = self
                        .provisional_primary
                        .as_ref()
                        .ok_or(LinkError::Protocol)?;
                    let requested = provisional
                        .end_offset
                        .checked_next_request()
                        .ok_or(LinkError::Protocol)?;
                    actions.push(ReplicaAction::SendDualPsync(protocol::build_psync(
                        &provisional.replid,
                        requested.get(),
                    )));
                    crate::server_log!(
                        debug,
                        "dual-rdb main handshake: queued SendDualPsync offset={}",
                        requested.get()
                    );
                    self.state = ReplicaLinkState::WaitPsyncReply;
                }
                ReplicaLinkState::WaitPsyncReply => {
                    let Some(line) = take_line(&mut self.input) else {
                        break;
                    };
                    if protocol::parse_dual_channel_sync(&line) {
                        self.full_resync_accepted = true;
                        self.mark_failover_accepted();
                        // The main PSYNC can continue before the direct RDB
                        // child writes its header. Do not let that window
                        // emit an ACK that would pre-arm primary promotion.
                        self.ack_enabled_after_rdb_commit = false;
                        self.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)?;
                        self.rdb_auth_pending = self.primary_auth.is_some();
                        let mut initial_frames = Vec::new();
                        if let Some(password) = self.primary_auth.as_deref() {
                            initial_frames
                                .push(protocol::build_auth(self.primary_user.as_deref(), password));
                        }
                        initial_frames.push(protocol::build_replconf_rdb_channel_with_compression(
                            &self.compression_mode,
                        ));
                        actions.push(ReplicaAction::OpenRdbChannel { initial_frames });
                    } else if let Some(fullresync) = protocol::parse_fullresync(&line) {
                        self.mark_failover_accepted();
                        if !self.main_wire_active {
                            self.main_wire_active = true;
                            self.pending_wire_input_bytes += self.input.len() as u64;
                        }
                        self.begin_rdb()?;
                        self.full_resync_accepted = true;
                        self.master_replid = Some(fullresync.replid);
                        self.requested_offset = Some(fullresync.offset);
                        self.received_offset = fullresync.offset;
                        self.decoded_offset = fullresync.offset;
                        // C records primary_initial_offset separately from
                        // the live or cached primary's processed position.
                        // Publish this snapshot only after the RDB commits.
                        // A full resync begins a fresh stream that re-emits SELECT
                        // and reloads from the RDB, so any cached resume DB is
                        // discarded and we restart on DB 0 (§5.6).
                        self.selected_db = 0;
                    } else if let Some(new_replid) = protocol::parse_continue(&line) {
                        if self.master_replid.is_none() || self.requested_offset.is_none() {
                            crate::server_log!(
                                warning,
                                "Primary replied +CONTINUE to a full resynchronization request, will try later."
                            );
                            return Err(LinkError::TryLater);
                        }
                        self.mark_failover_accepted();
                        self.partial_resync_accepted = true;
                        if let Some(rid) = new_replid {
                            let primary_link_is_classic =
                                self.rdb_channel_state == ReplicaRdbChannelState::None;
                            if primary_link_is_classic
                                && self.master_replid.as_deref() != Some(rid.as_str())
                            {
                                self.pending_primary_replid_change = Some(rid.clone());
                            }
                            self.master_replid = Some(rid);
                        }
                        self.state = ReplicaLinkState::Streaming;
                        if !self.main_wire_active {
                            self.main_wire_active = true;
                            self.pending_wire_input_bytes += self.input.len() as u64;
                        }
                    } else if self.failover_request {
                        self.failover_request = false;
                        return Err(LinkError::FailoverRejected);
                    } else if line.starts_with(b"-NOMASTERLINK")
                        || line.starts_with(b"-LOADING")
                        || line.starts_with(b"-BUSY")
                    {
                        // C's replicaProcessPsyncReply() classifies these replies
                        // as PSYNC_TRY_LATER: the primary is temporarily unable
                        // to serve PSYNC, not speaking an invalid protocol.
                        crate::server_log!(
                            notice,
                            "Primary is currently unable to PSYNC but should be in the future: {}",
                            String::from_utf8_lossy(&line)
                        );
                        return Err(LinkError::TryLater);
                    } else {
                        return Err(LinkError::Protocol);
                    }
                }
                ReplicaLinkState::ReceivingRdbHeader => {
                    let Some(header) = take_line(&mut self.input) else {
                        break;
                    };
                    self.state = parse_rdb_header(&header)?;
                    self.begin_rdb_receive();
                }
                ReplicaLinkState::ReceivingRdb { remaining } => {
                    if remaining == 0 {
                        self.state = ReplicaLinkState::Streaming;
                        continue;
                    }
                    if self.input.is_empty() {
                        break;
                    }
                    let count = remaining.min(self.input.len());
                    let body: Vec<u8> = self.input.drain(..count).collect();
                    if let Some(recv) = self.rdb_receive.as_mut() {
                        recv.feed(&body);
                    }
                    self.state = ReplicaLinkState::ReceivingRdb {
                        remaining: remaining - count,
                    };
                }
                ReplicaLinkState::ReceivingRdbEof { marker } => {
                    if self.input.is_empty() {
                        break;
                    }
                    let mut window = std::mem::take(&mut self.eof_holdback);
                    window.append(&mut self.input);
                    if let Some(position) = window
                        .windows(marker.len())
                        .position(|chunk| chunk == marker)
                    {
                        if position > 0 {
                            if let Some(recv) = self.rdb_receive.as_mut() {
                                recv.feed(&window[..position]);
                            }
                        }
                        self.input = window.split_off(position + marker.len());
                        self.state = ReplicaLinkState::Streaming;
                    } else {
                        let hold = marker.len().saturating_sub(1).min(window.len());
                        let feed_len = window.len() - hold;
                        if feed_len > 0 {
                            if let Some(recv) = self.rdb_receive.as_mut() {
                                recv.feed(&window[..feed_len]);
                            }
                        }
                        self.eof_holdback = window.split_off(feed_len);
                        break;
                    }
                }
                ReplicaLinkState::Streaming => {
                    let is_dual = self.rdb_channel_state != ReplicaRdbChannelState::None;
                    if self.pending_stream_decompression_error {
                        self.input.clear();
                        break;
                    }
                    if let Some(decoder) = self.compression_decoder.as_mut() {
                        match decoder
                            .feed(&std::mem::take(&mut self.input), REPLICATION_BATCH_BYTES)
                        {
                            Ok(decoded) => self.input = decoded,
                            Err(_)
                                if self.rdb_receive.is_some()
                                    || (is_dual
                                        && self.rdb_channel_state
                                            != ReplicaRdbChannelState::RdbLoaded) =>
                            {
                                self.pending_stream_decompression_error = true;
                                self.compression_decoder = None;
                                break;
                            }
                            Err(_) => {
                                if is_dual {
                                    crate::server_log!(
                                        warning,
                                        "Dual-channel replication stream decompression failure"
                                    );
                                } else {
                                    crate::server_log!(
                                        warning,
                                        "replication stream decompression failure"
                                    );
                                }
                                return Err(LinkError::Protocol);
                            }
                        }
                    }
                    if self.input.is_empty() {
                        break;
                    }
                    let received_offset = match prevalidated_streaming_received_offset {
                        Some(offset) => offset,
                        None => self.prevalidate_streaming_input(self.input.len())?,
                    };
                    self.parser.feed(&self.input);
                    self.input.clear();
                    let mut decoded_offset = self.decoded_offset;
                    let mut decoded_events = Vec::new();
                    loop {
                        let parts = self.parser.try_parse().map_err(|_| LinkError::Protocol)?;
                        let Some(parts) = parts else {
                            break;
                        };
                        decoded_offset = decoded_offset
                            .checked_advance(self.parser.last_cmd_len())
                            .ok_or(LinkError::Protocol)?;
                        let args = CommandArgs::from_parts(parts);
                        let apply = classify_command(args)?;
                        decoded_events.push(ReplicaEvent::Apply {
                            apply,
                            end_offset: decoded_offset,
                        });
                    }
                    self.received_offset = received_offset;
                    self.decoded_offset = decoded_offset;
                    events.extend(decoded_events);
                    break;
                }
            }
        }

        Ok((events, actions))
    }

    fn prevalidate_streaming_input(
        &self,
        input_len: usize,
    ) -> Result<ReplicationOffset, LinkError> {
        let undecoded_len = self
            .parser
            .undecoded_len()
            .checked_add(input_len)
            .ok_or(LinkError::Protocol)?;
        self.decoded_offset
            .checked_advance(undecoded_len)
            .ok_or(LinkError::Protocol)?;
        self.received_offset
            .checked_advance(input_len)
            .ok_or(LinkError::Protocol)
    }

    pub(crate) fn has_buffered_input(&self) -> bool {
        !self.input.is_empty() || self.has_pending_decompression()
    }

    fn drive_rdb_framing_once(&mut self) -> Result<FramingAction, LinkError> {
        match self.state {
            ReplicaLinkState::ReceivingRdbHeader => {
                let Some(header) = take_line(&mut self.input) else {
                    return Ok(FramingAction::Break);
                };
                self.state = parse_rdb_header(&header)?;
                self.begin_rdb_receive();
                Ok(FramingAction::Continue)
            }
            ReplicaLinkState::ReceivingRdb { remaining } => {
                if remaining == 0 {
                    self.state = ReplicaLinkState::Streaming;
                    return Ok(FramingAction::Continue);
                }
                if self.input.is_empty() {
                    return Ok(FramingAction::Break);
                }
                let count = remaining.min(self.input.len());
                let body: Vec<u8> = self.input.drain(..count).collect();
                if let Some(recv) = self.rdb_receive.as_mut() {
                    recv.feed(&body);
                }
                self.state = ReplicaLinkState::ReceivingRdb {
                    remaining: remaining - count,
                };
                Ok(FramingAction::Continue)
            }
            ReplicaLinkState::ReceivingRdbEof { marker } => {
                if self.input.is_empty() {
                    return Ok(FramingAction::Break);
                }
                let mut window = std::mem::take(&mut self.eof_holdback);
                window.append(&mut self.input);
                if let Some(position) = window
                    .windows(marker.len())
                    .position(|chunk| chunk == marker)
                {
                    if position > 0 {
                        if let Some(recv) = self.rdb_receive.as_mut() {
                            recv.feed(&window[..position]);
                        }
                    }
                    self.input = window.split_off(position + marker.len());
                    self.state = ReplicaLinkState::Streaming;
                    Ok(FramingAction::Continue)
                } else {
                    let hold = marker.len().saturating_sub(1).min(window.len());
                    let feed_len = window.len() - hold;
                    if feed_len > 0 {
                        if let Some(recv) = self.rdb_receive.as_mut() {
                            recv.feed(&window[..feed_len]);
                        }
                    }
                    self.eof_holdback = window.split_off(feed_len);
                    Ok(FramingAction::Break)
                }
            }
            _ => Ok(FramingAction::Break),
        }
    }

    #[allow(dead_code)]
    pub fn on_rdb_channel_framing_bytes(
        &mut self,
        bytes: &[u8],
    ) -> Result<(Vec<ReplicaEvent>, Vec<ReplicaAction>), LinkError> {
        self.last_io_time_ms = crate::foundation::io::sys::time::mstime();
        self.input.extend_from_slice(bytes);
        let events = Vec::new();
        let actions = Vec::new();

        if !matches!(
            self.state,
            ReplicaLinkState::ReceivingRdbHeader
                | ReplicaLinkState::ReceivingRdb { .. }
                | ReplicaLinkState::ReceivingRdbEof { .. }
        ) {
            self.state = ReplicaLinkState::ReceivingRdbHeader;
        }

        loop {
            if !matches!(
                self.state,
                ReplicaLinkState::ReceivingRdbHeader
                    | ReplicaLinkState::ReceivingRdb { .. }
                    | ReplicaLinkState::ReceivingRdbEof { .. }
            ) {
                break;
            }
            let action = self.drive_rdb_framing_once()?;
            match action {
                FramingAction::Continue => continue,
                FramingAction::Break => break,
            }
        }

        Ok((events, actions))
    }

    pub fn on_rdb_bytes(
        &mut self,
        bytes: &[u8],
    ) -> Result<(Vec<ReplicaEvent>, Vec<ReplicaAction>), LinkError> {
        if self.rdb_wire_active {
            self.pending_wire_input_bytes += bytes.len() as u64;
        }
        self.last_io_time_ms = crate::foundation::io::sys::time::mstime();
        self.rdb_input.extend_from_slice(bytes);
        let events = Vec::new();
        let mut actions = Vec::new();

        while self.rdb_framing_state.is_none() {
            match self.rdb_channel_state {
                ReplicaRdbChannelState::Handshake => {
                    let Some(line) = take_line(&mut self.rdb_input) else {
                        return Ok((events, actions));
                    };
                    crate::server_log!(
                        debug,
                        "dual-rdb handshake: state=Handshake line={:?}",
                        String::from_utf8_lossy(&line)
                    );
                    if line != b"+OK" {
                        return Err(LinkError::Protocol);
                    }
                    if self.rdb_auth_pending {
                        self.rdb_auth_pending = false;
                        continue;
                    }
                    self.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)?;
                }
                ReplicaRdbChannelState::WaitingForEndOffset => {
                    let mut keepalive_bytes = 0;
                    loop {
                        let pending = &self.rdb_input[keepalive_bytes..];
                        if pending.starts_with(b"\n") {
                            keepalive_bytes += 1;
                        } else if pending.starts_with(b"\r\n") {
                            keepalive_bytes += 2;
                        } else {
                            break;
                        }
                    }
                    self.rdb_input.drain(..keepalive_bytes);
                    let Some(line) = take_line(&mut self.rdb_input) else {
                        return Ok((events, actions));
                    };
                    crate::server_log!(
                        debug,
                        "dual-rdb handshake: state=WaitingForEndOffset line={:?}",
                        String::from_utf8_lossy(&line)
                    );
                    let Some(endoff) = protocol::parse_end_offset(&line) else {
                        return Err(LinkError::Protocol);
                    };
                    self.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)?;
                    self.provisional_primary = Some(ProvisionalPrimary {
                        replid: endoff.replid.clone(),
                        end_offset: endoff.offset,
                        db_id: endoff.db_id,
                        rdb_client_id: endoff.client_id,
                    });
                    // Seed the main-stream cursor before +CONTINUE while
                    // retaining the processed history until the RDB commits.
                    self.master_replid = Some(endoff.replid.clone());
                    self.requested_offset = Some(endoff.offset);
                    self.received_offset = endoff.offset;
                    self.decoded_offset = endoff.offset;
                    self.selected_db = endoff.db_id;
                    self.state = ReplicaLinkState::WaitDualRdbClientIdReply;
                    self.rdb_framing_state = Some(RdbFramingState::Header);
                    if !self.rdb_wire_active {
                        self.rdb_wire_active = true;
                        self.pending_wire_input_bytes += self.rdb_input.len() as u64;
                    }
                    actions.push(ReplicaAction::Send(
                        protocol::build_replconf_set_rdb_client_id(endoff.client_id),
                    ));
                }
                _ => return Err(LinkError::Protocol),
            }
        }

        while self.drive_rdb_channel_framing_once()? {}

        Ok((events, actions))
    }

    fn drive_rdb_channel_framing_once(&mut self) -> Result<bool, LinkError> {
        let Some(framing_state) = self.rdb_framing_state else {
            return Ok(false);
        };
        match framing_state {
            RdbFramingState::Header => {
                let Some(header) = take_line(&mut self.rdb_input) else {
                    return Ok(false);
                };
                self.rdb_framing_state = Some(match parse_rdb_header(&header)? {
                    ReplicaLinkState::ReceivingRdb { remaining } => {
                        RdbFramingState::Body { remaining }
                    }
                    ReplicaLinkState::ReceivingRdbEof { marker } => RdbFramingState::Eof { marker },
                    _ => return Err(LinkError::Protocol),
                });
                self.begin_rdb_receive();
                Ok(true)
            }
            RdbFramingState::Body { remaining } => {
                if remaining == 0 {
                    return Ok(false);
                }
                if self.rdb_input.is_empty() {
                    return Ok(false);
                }
                let count = remaining.min(self.rdb_input.len());
                let body: Vec<u8> = self.rdb_input.drain(..count).collect();
                if let Some(recv) = self.rdb_receive.as_mut() {
                    recv.feed(&body);
                }
                self.rdb_framing_state = Some(RdbFramingState::Body {
                    remaining: remaining - count,
                });
                Ok(remaining != count)
            }
            RdbFramingState::Eof { marker } => {
                if self.rdb_input.is_empty() {
                    return Ok(false);
                }
                let mut window = std::mem::take(&mut self.rdb_eof_holdback);
                window.append(&mut self.rdb_input);
                if let Some(position) = window
                    .windows(marker.len())
                    .position(|chunk| chunk == marker)
                {
                    if position > 0 {
                        if let Some(recv) = self.rdb_receive.as_mut() {
                            recv.feed(&window[..position]);
                        }
                    }
                    self.rdb_input = window.split_off(position + marker.len());
                    self.rdb_framing_state = None;
                    crate::server_log!(
                        debug,
                        "dual-rdb rdb EOF marker received: main={} trailing_main_bytes={}",
                        self.connection_id.0,
                        self.rdb_input.len()
                    );
                    Ok(false)
                } else {
                    let hold = marker.len().saturating_sub(1).min(window.len());
                    let feed_len = window.len() - hold;
                    if feed_len > 0 {
                        if let Some(recv) = self.rdb_receive.as_mut() {
                            recv.feed(&window[..feed_len]);
                        }
                    }
                    self.rdb_eof_holdback = window.split_off(feed_len);
                    Ok(false)
                }
            }
        }
    }

    pub fn is_dual_rdb_mode_active(&self) -> bool {
        matches!(
            self.rdb_channel_state,
            ReplicaRdbChannelState::ReceivingRdb | ReplicaRdbChannelState::RdbLoaded
        ) || self.provisional_primary.is_some()
    }

    fn begin_rdb(&mut self) -> Result<(), LinkError> {
        if self.is_dual_rdb_mode_active() {
            return Err(LinkError::Protocol);
        }
        let Some(header) = take_line(&mut self.input) else {
            self.state = ReplicaLinkState::ReceivingRdbHeader;
            return Ok(());
        };
        self.state = parse_rdb_header(&header)?;
        self.begin_rdb_receive();
        Ok(())
    }

    /// Allocate the streaming decoder for a fresh full-sync RDB phase. Called
    /// once the header line pins the framing mode (length-framed vs. diskless
    /// EOF-marker), so the diskless flag is read straight off the state. The
    /// runtime can request raw retention when the configured replica load mode
    /// must persist the transfer before installing it.
    fn begin_rdb_receive(&mut self) {
        self.ack_enabled_after_rdb_commit = false;
        let diskless = matches!(self.rdb_framing_state, Some(RdbFramingState::Eof { .. }))
            || matches!(self.state, ReplicaLinkState::ReceivingRdbEof { .. });
        let load_mode = *self.rdb_load_mode.get_or_insert(RdbLoadMode::Direct);
        self.rdb_receive =
            Some(RdbReceive::new(diskless, load_mode).with_skip_checksums(self.rdb_skip_checksums));
        if let (Some(receive), Some(options)) =
            (self.rdb_receive.as_mut(), self.rdb_snapshot_options)
        {
            receive.set_snapshot_options(options);
        }
    }

    /// Mutable access to the streaming receive state so the backend can drive the
    /// paced decode against the serving/temp `DbSet` it owns (see
    /// `rdb_recv_path.rs`). `None` outside an RDB phase.
    pub fn rdb_receive_mut(&mut self) -> Option<&mut RdbReceive> {
        self.rdb_receive.as_mut()
    }

    pub fn pin_rdb_load_mode(&mut self, load_mode: RdbLoadMode) {
        self.rdb_load_mode.get_or_insert(load_mode);
    }

    pub fn rdb_load_mode(&self) -> Option<RdbLoadMode> {
        self.rdb_load_mode
    }

    pub fn rdb_receive_preserves_serving_db(&self) -> bool {
        self.rdb_receive
            .as_ref()
            .is_some_and(RdbReceive::preserves_serving_db)
    }

    pub fn has_received_rdb_payload(&self) -> bool {
        self.rdb_receive
            .as_ref()
            .is_some_and(RdbReceive::has_received_payload)
    }

    pub fn rdb_payload_bytes(&self) -> u64 {
        self.rdb_receive
            .as_ref()
            .map_or(0, RdbReceive::payload_bytes)
    }

    /// True while a streaming RDB decode is live (allocated but not yet committed
    /// at EOF). The bounded-read receive path stays engaged until this clears, so
    /// the command-stream drain loop never sees a half-loaded RDB.
    pub fn has_pending_rdb_receive(&self) -> bool {
        self.rdb_receive.is_some()
    }

    /// True while the RDB channel still owns framing. This is intentionally
    /// independent from the main-channel handshake state: dual-channel RDB can
    /// be decoded while the main link waits for REPLCONF's reply.
    pub fn rdb_framing_is_pending(&self) -> bool {
        matches!(
            self.rdb_framing_state,
            Some(RdbFramingState::Header)
                | Some(RdbFramingState::Body { remaining: 1.. })
                | Some(RdbFramingState::Eof { .. })
        )
    }

    /// True once an RDB-channel transfer has consumed its length or EOF
    /// framing. Decoder completion remains a separate condition.
    pub fn rdb_framing_is_complete(&self) -> bool {
        matches!(
            self.rdb_framing_state,
            Some(RdbFramingState::Body { remaining: 0 }) | None
        ) && self.rdb_channel_state == ReplicaRdbChannelState::ReceivingRdb
    }

    /// Take ownership of the receive state for the terminal commit once the
    /// decoder reaches EOF, leaving the link with no live RDB phase.
    pub fn take_rdb_receive(&mut self) -> Option<RdbReceive> {
        self.rdb_receive.take()
    }

    pub fn prevalidate_rdb_apply_position(&self) -> Result<ReplicationOffset, LinkError> {
        self.requested_offset.ok_or(LinkError::Protocol)
    }

    pub fn finish_rdb_apply(&mut self, apply_position: ReplicationOffset) {
        debug_assert_eq!(self.requested_offset, Some(apply_position));
        self.applied_offset = apply_position;
        self.ack_enabled_after_rdb_commit = !self.is_dual_rdb_mode_active();
        // Classic full sync arms the first periodic ACK immediately. Dual
        // channel keeps the gate closed until `acknowledge_applied` emits the
        // terminal ACK after pending replay and LOADING both finish. This is
        // the Rust equivalent of C resurrecting its provisional primary only
        // after the dual handoff is complete.
        self.last_periodic_ack_ms = 0;
    }

    pub fn reset_applied_offset_to_dual_snapshot(&mut self) {
        if let Some(primary) = self.provisional_primary.as_ref() {
            self.applied_offset = primary.end_offset;
        }
    }

    pub fn can_ack_primary(&self) -> bool {
        self.ack_enabled_after_rdb_commit
    }

    pub fn finish_command_apply(&mut self, end_offset: ReplicationOffset) -> Result<(), LinkError> {
        if end_offset < self.applied_offset || end_offset > self.received_offset {
            return Err(LinkError::Apply);
        }
        self.applied_offset = end_offset;
        Ok(())
    }

    pub fn acknowledge_applied(&mut self, aof_offset: Option<ReplicationOffset>) -> ReplicaAction {
        // GETACK callers already require `can_ack_primary`; the only call that
        // can reach this with a closed gate is the explicit terminal handoff.
        self.ack_enabled_after_rdb_commit = true;
        self.acked_offset = self.applied_offset;
        ReplicaAction::Send(protocol::build_replconf_ack(self.acked_offset, aof_offset))
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub(crate) fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn master(&self) -> &MasterInfo {
        &self.master
    }

    #[allow(dead_code)] // Wired into dual-channel negotiation in a later slice.
    pub fn rdb_channel_state(&self) -> ReplicaRdbChannelState {
        self.rdb_channel_state
    }

    #[allow(dead_code)] // Wired into dual-channel negotiation in a later slice.
    pub fn transition_rdb_channel_state(
        &mut self,
        next: ReplicaRdbChannelState,
    ) -> Result<(), LinkError> {
        self.rdb_channel_state = self.rdb_channel_state.transition(next)?;
        Ok(())
    }

    #[cfg(test)]
    fn state(&self) -> ReplicaLinkState {
        self.state
    }

    pub fn cluster_node_id(&self) -> Option<&str> {
        self.cluster_node_id.as_deref()
    }

    pub fn set_selected_db(&mut self, selected_db: u32) {
        self.selected_db = selected_db;
    }

    pub fn selected_db(&self) -> u32 {
        self.selected_db
    }

    pub fn master_replid(&self) -> Option<&str> {
        self.master_replid.as_deref()
    }

    pub fn take_primary_replid_change(&mut self) -> Option<String> {
        self.pending_primary_replid_change.take()
    }

    pub const fn applied_position(&self) -> ReplicationOffset {
        self.applied_offset
    }

    pub fn is_streaming(&self) -> bool {
        matches!(self.state, ReplicaLinkState::Streaming)
    }

    /// A diskless EOF marker advances the protocol state to `Streaming` before
    /// the RDB decoder commits the loaded dataset. A disconnect in that window
    /// cannot safely resume from the RDB's advertised offset.
    pub fn can_resume_after_disconnect(&self) -> bool {
        self.is_streaming()
            && self.ack_enabled_after_rdb_commit
            && !self.rdb_transfer_failed
            && !self.has_pending_rdb_receive()
            && matches!(
                self.rdb_channel_state,
                ReplicaRdbChannelState::None | ReplicaRdbChannelState::RdbLoaded
            )
    }

    pub fn mark_rdb_transfer_failed(&mut self) {
        if self.rdb_channel_state != ReplicaRdbChannelState::RdbLoaded {
            self.rdb_transfer_failed = true;
        }
    }

    pub fn is_in_handshake(&self) -> bool {
        !matches!(
            self.state,
            ReplicaLinkState::Streaming
                | ReplicaLinkState::ReceivingRdbHeader
                | ReplicaLinkState::ReceivingRdb { .. }
                | ReplicaLinkState::ReceivingRdbEof { .. }
        )
    }

    pub fn mark_dual_main_psync_paused(&mut self) {
        self.dual_main_psync_paused = true;
    }

    pub fn dual_main_psync_was_paused(&self) -> bool {
        self.dual_main_psync_paused
    }

    pub fn is_receiving_rdb(&self) -> bool {
        matches!(
            self.state,
            ReplicaLinkState::ReceivingRdbHeader
                | ReplicaLinkState::ReceivingRdb { .. }
                | ReplicaLinkState::ReceivingRdbEof { .. }
        )
    }

    pub const fn received_position(&self) -> ReplicationOffset {
        self.received_offset
    }

    pub fn last_io_time_ms(&self) -> i64 {
        self.last_io_time_ms
    }

    pub fn mark_io_activity(&mut self) {
        self.last_io_time_ms = crate::foundation::io::sys::time::mstime();
    }

    #[cfg(test)]
    pub fn set_last_io_time_ms(&mut self, value: i64) {
        self.last_io_time_ms = value;
    }
    pub fn defer_events(&mut self, events: Vec<ReplicaEvent>) {
        self.deferred_events.extend(events);
    }

    pub fn take_deferred_events(&mut self) -> Vec<ReplicaEvent> {
        self.deferred_events.drain(..).collect()
    }

    pub const fn acknowledged_position(&self) -> ReplicationOffset {
        self.acked_offset
    }

    /// Produce a periodic REPLCONF ACK if the replica is in streaming state
    /// and at least 1s has elapsed since the last periodic ACK. Mirrors C's
    /// `replicationSendAck` in replicationCron (replication.c), which keeps
    /// the primary's `repl_ack_time` fresh so the "(streaming sync)" timeout
    /// path can detect silent peers.
    pub fn take_periodic_ack(
        &mut self,
        now_ms: i64,
        aof_offset: Option<ReplicationOffset>,
    ) -> Option<ReplicaAction> {
        if !matches!(self.state, ReplicaLinkState::Streaming) || !self.ack_enabled_after_rdb_commit
        {
            return None;
        }
        const PERIODIC_ACK_INTERVAL_MS: i64 = 1_000;
        if now_ms.saturating_sub(self.last_periodic_ack_ms) < PERIODIC_ACK_INTERVAL_MS {
            return None;
        }
        self.acked_offset = self.applied_offset;
        self.last_periodic_ack_ms = now_ms;
        Some(ReplicaAction::Send(protocol::build_replconf_ack(
            self.acked_offset,
            aof_offset,
        )))
    }

    #[cfg(test)]
    pub(crate) fn input_bytes_flattened(&self) -> Vec<u8> {
        self.input.clone()
    }
}

fn classify_command(args: CommandArgs) -> Result<ReplicationApply, LinkError> {
    let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
    match name.as_str() {
        "select" => {
            let db = args
                .args
                .first()
                .and_then(|arg| std::str::from_utf8(arg).ok())
                .and_then(|arg| arg.parse().ok())
                .ok_or(LinkError::Protocol)?;
            Ok(ReplicationApply::Select(db))
        }
        "ping" => Ok(ReplicationApply::Ping),
        "replconf"
            if args
                .args
                .first()
                .is_some_and(|arg| arg.eq_ignore_ascii_case(b"getack")) =>
        {
            Ok(ReplicationApply::GetAck)
        }
        _ => Ok(ReplicationApply::Command(args)),
    }
}

fn parse_rdb_header(header: &[u8]) -> Result<ReplicaLinkState, LinkError> {
    let raw = header.strip_prefix(b"$").ok_or(LinkError::Protocol)?;
    if let Some(marker) = raw.strip_prefix(b"EOF:") {
        if marker.len() != 40 {
            return Err(LinkError::Protocol);
        }
        return Ok(ReplicaLinkState::ReceivingRdbEof {
            marker: marker.try_into().map_err(|_| LinkError::Protocol)?,
        });
    }
    let length = std::str::from_utf8(raw)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .ok_or(LinkError::Protocol)?;
    Ok(ReplicaLinkState::ReceivingRdb { remaining: length })
}

fn take_line(input: &mut Vec<u8>) -> Option<Vec<u8>> {
    let end = input.windows(2).position(|window| window == b"\r\n")?;
    let line = input[..end].to_vec();
    input.drain(..end + 2);
    Some(line)
}

enum FramingAction {
    Continue,
    Break,
}

#[cfg(test)]
#[path = "replica_link/tests.rs"]
mod tests;
