use crate::framework::context::ServerContext;

pub(super) fn config_set_misc(
    _ctx: &mut ServerContext,
    param: &str,
    val_trimmed: &str,
    _val_bytes: &[u8],
) -> Result<(), String> {
    // Upstream's commandlog coverage clears these sensitive credentials even
    // when TLS is disabled. Clearing an absent TLS secret is a safe no-op;
    // accepting a non-empty secret would misleadingly claim TLS support.
    if matches!(param, "tls-key-file-pass" | "tls-client-key-file-pass") && val_trimmed.is_empty()
    {
        return Ok(());
    }
    if param.starts_with("tls-") {
        return Err("ERR TLS is not supported by this build".to_string());
    }
    Err(format!("ERR CONFIG SET failed (unknown option: {})", param))
}
