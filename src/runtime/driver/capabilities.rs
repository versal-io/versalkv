#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    pub timers: bool,
    pub blocking: bool,
    pub connections: bool,
    pub persistence: bool,
    pub process_control: bool,
    pub distributed: bool,
    pub hosted_tasks: bool,
}

impl RuntimeCapabilities {
    pub const fn server() -> Self {
        Self {
            timers: true,
            blocking: true,
            connections: true,
            persistence: true,
            process_control: true,
            distributed: true,
            hosted_tasks: true,
        }
    }

    pub const fn embedded() -> Self {
        Self {
            timers: true,
            blocking: false,
            connections: false,
            persistence: false,
            process_control: false,
            distributed: false,
            hosted_tasks: false,
        }
    }
}
