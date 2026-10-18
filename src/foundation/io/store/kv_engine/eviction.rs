#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EvictionPolicy {
    VolatileLru,
    AllkeysLru,
    VolatileLfu,
    AllkeysLfu,
    VolatileRandom,
    AllkeysRandom,
    VolatileTtl,
    #[default]
    NoEviction,
}

impl EvictionPolicy {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "volatile-lru" => Some(Self::VolatileLru),
            "allkeys-lru" => Some(Self::AllkeysLru),
            "volatile-lfu" => Some(Self::VolatileLfu),
            "allkeys-lfu" => Some(Self::AllkeysLfu),
            "volatile-random" => Some(Self::VolatileRandom),
            "allkeys-random" => Some(Self::AllkeysRandom),
            "volatile-ttl" => Some(Self::VolatileTtl),
            "noeviction" => Some(Self::NoEviction),
            _ => None,
        }
    }

    pub fn is_volatile(self) -> bool {
        matches!(
            self,
            Self::VolatileLru | Self::VolatileLfu | Self::VolatileRandom | Self::VolatileTtl
        )
    }

    pub fn uses_lfu(self) -> bool {
        matches!(self, Self::VolatileLfu | Self::AllkeysLfu)
    }

    pub fn is_random(self) -> bool {
        matches!(self, Self::VolatileRandom | Self::AllkeysRandom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvictionCandidate {
    pub key: Vec<u8>,
    pub score: u64,
}
