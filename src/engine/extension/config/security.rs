#[derive(Clone)]
pub struct SecurityConfig {
    pub requirepass: Option<Vec<u8>>,
    pub primaryauth: Option<Vec<u8>>,
    pub primaryuser: Option<Vec<u8>>,
}

impl SecurityConfig {
    pub fn new() -> Self {
        Self {
            requirepass: None,
            primaryauth: None,
            primaryuser: None,
        }
    }
}
