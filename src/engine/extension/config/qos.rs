use std::net::IpAddr;

#[derive(Clone)]
pub struct PrioritySubnet {
    address: IpAddr,
    prefix_bits: u32,
}

impl PrioritySubnet {
    fn parse(value: &str) -> Result<Self, String> {
        let (address, prefix) = value
            .split_once('/')
            .map_or((value, None), |(address, prefix)| (address, Some(prefix)));
        let mut address = address.parse::<IpAddr>().map_err(|_| invalid_subnet())?;
        let address_bits = if address.is_ipv4() { 32 } else { 128 };
        let mut prefix_bits = match prefix {
            Some(prefix) => prefix.parse::<u32>().map_err(|_| invalid_subnet())?,
            None => address_bits,
        };
        if prefix_bits > address_bits {
            return Err(invalid_subnet());
        }
        if let IpAddr::V6(ipv6) = address {
            if let Some(ipv4) = ipv6.to_ipv4_mapped() {
                address = IpAddr::V4(ipv4);
                prefix_bits = prefix_bits.saturating_sub(96);
            }
        }
        Ok(Self {
            address,
            prefix_bits,
        })
    }

    pub fn contains(&self, address: IpAddr) -> bool {
        let address = match address {
            IpAddr::V6(ipv6) => ipv6.to_ipv4_mapped().map_or(IpAddr::V6(ipv6), IpAddr::V4),
            address => address,
        };
        match (self.address, address) {
            (IpAddr::V4(subnet), IpAddr::V4(address)) => {
                let mask = u32::MAX.checked_shl(32 - self.prefix_bits).unwrap_or(0);
                u32::from(subnet) & mask == u32::from(address) & mask
            }
            (IpAddr::V6(subnet), IpAddr::V6(address)) => {
                let mask = u128::MAX.checked_shl(128 - self.prefix_bits).unwrap_or(0);
                u128::from(subnet) & mask == u128::from(address) & mask
            }
            _ => false,
        }
    }
}

pub fn parse_priority_subnets(value: &str) -> Result<Vec<PrioritySubnet>, String> {
    value
        .split(|character: char| character == ',' || character.is_ascii_whitespace())
        .filter(|source| !source.is_empty())
        .map(PrioritySubnet::parse)
        .collect()
}

fn invalid_subnet() -> String {
    "Invalid IP address or CIDR subnet in priority-subnets".to_string()
}

pub(crate) fn parse_priority_numeric_value(name: &str, value: &str) -> Option<Result<i64, String>> {
    let maximum = match name {
        "maxclients-reserved" => u32::MAX as i64,
        "priority-preemptive-poll-interval-us" => i32::MAX as i64,
        _ => return None,
    };
    Some((|| {
        let parsed = value
            .parse::<i64>()
            .ok()
            .filter(|number| number.to_string() == value)
            .ok_or_else(|| "ERR argument couldn't be parsed into an integer".to_string())?;
        if !(0..=maximum).contains(&parsed) {
            return Err(format!("ERR argument must be between 0 and {maximum}"));
        }
        Ok(parsed)
    })())
}
