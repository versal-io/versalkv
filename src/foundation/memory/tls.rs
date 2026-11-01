#[derive(Clone, Default, PartialEq, Eq)]
pub struct TlsPassword(String);

impl TlsPassword {
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl From<String> for TlsPassword {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl std::fmt::Debug for TlsPassword {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[redacted]")
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsCertificateInfo {
    pub serial: Option<String>,
    pub expires_at_sec: Option<i64>,
}

impl TlsCertificateInfo {
    pub fn expires_in_seconds(&self, now_sec: i64) -> i64 {
        self.expires_at_sec.map_or(0, |expiry| expiry - now_sec)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsInfo {
    pub server: TlsCertificateInfo,
    pub alternate: TlsCertificateInfo,
    pub client: TlsCertificateInfo,
    pub ca: TlsCertificateInfo,
}
