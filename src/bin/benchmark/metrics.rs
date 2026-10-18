#[derive(Default, Clone, Debug)]
pub struct LatencyRecorder {
    samples_us: Vec<u64>,
}

impl LatencyRecorder {
    pub fn record(&mut self, latency_us: u64) {
        self.samples_us.push(latency_us);
    }

    pub fn append(&mut self, other: &mut Self) {
        self.samples_us.append(&mut other.samples_us);
    }

    pub fn sort_unstable(&mut self) {
        self.samples_us.sort_unstable();
    }

    pub fn percentile_sorted(&self, pct: f64) -> u64 {
        percentile(&self.samples_us, pct)
    }
}

#[derive(Clone, Debug)]
pub struct CommandStats {
    pub name: String,
    pub requests: u64,
    pub errors: u64,
    pub rps: f64,
    pub percentiles: Vec<(f64, u64)>,
}

#[derive(Clone, Debug)]
pub struct BenchmarkSummary {
    pub total_requests: u64,
    pub total_errors: u64,
    pub duration_secs: f64,
    pub commands: Vec<CommandStats>,
}

impl BenchmarkSummary {
    pub fn to_json_string(&self) -> String {
        let mut out = String::new();
        out.push('{');
        out.push_str("\"total_requests\":");
        out.push_str(&self.total_requests.to_string());
        out.push_str(",\"total_errors\":");
        out.push_str(&self.total_errors.to_string());
        out.push_str(",\"duration_secs\":");
        out.push_str(&json_number(self.duration_secs));
        out.push_str(",\"commands\":[");
        for (idx, command) in self.commands.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            command.write_json(&mut out);
        }
        out.push_str("]}");
        out
    }
}

impl CommandStats {
    fn write_json(&self, out: &mut String) {
        out.push('{');
        out.push_str("\"name\":\"");
        out.push_str(&json_escape(&self.name));
        out.push_str("\",\"requests\":");
        out.push_str(&self.requests.to_string());
        out.push_str(",\"errors\":");
        out.push_str(&self.errors.to_string());
        out.push_str(",\"rps\":");
        out.push_str(&json_number(self.rps));
        out.push_str(",\"percentiles\":[");
        for (idx, (pct, latency_us)) in self.percentiles.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            out.push_str("{\"percentile\":");
            out.push_str(&json_number(*pct));
            out.push_str(",\"latency_us\":");
            out.push_str(&latency_us.to_string());
            out.push('}');
        }
        out.push_str("]}");
    }
}

pub fn percentile(sorted_samples_us: &[u64], pct: f64) -> u64 {
    if sorted_samples_us.is_empty() {
        return 0;
    }
    let rank = ((sorted_samples_us.len() as f64) * pct / 100.0).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted_samples_us.len() - 1);
    sorted_samples_us[idx]
}

fn json_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}

fn json_number(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else {
        "0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_uses_sorted_latency_samples() {
        let mut rec = LatencyRecorder::default();
        for value in [10, 20, 30, 40] {
            rec.record(value);
        }
        rec.sort_unstable();
        assert_eq!(rec.percentile_sorted(50.0), 20);
        assert_eq!(rec.percentile_sorted(99.0), 40);
    }

    #[test]
    fn json_summary_contains_totals_and_commands() {
        let summary = BenchmarkSummary {
            total_requests: 2,
            total_errors: 0,
            duration_secs: 1.0,
            commands: vec![CommandStats {
                name: "GET".to_string(),
                requests: 2,
                errors: 0,
                rps: 2.0,
                percentiles: vec![(50.0, 100)],
            }],
        };
        let json = summary.to_json_string();
        assert!(json.contains("\"total_requests\":2"));
        assert!(json.contains("\"name\":\"GET\""));
    }
}
