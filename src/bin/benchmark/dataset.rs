use std::collections::HashMap;
use std::fs;

use super::cli::BenchConfig;
use super::templates::CommandPlan;

#[derive(Clone, Debug)]
pub struct Dataset {
    fields: Vec<String>,
    rows: Vec<Vec<String>>,
    index: HashMap<String, usize>,
}

impl Dataset {
    pub fn from_config(config: &BenchConfig, plan: &CommandPlan) -> Result<Option<Self>, String> {
        let Some(path) = &config.dataset else {
            return Ok(None);
        };
        if !plan.has_field_placeholders() {
            return Err("Dataset mode requires a command with field placeholders".to_string());
        }
        let dataset = Self::load(path, config.maxdocs)?;
        for field in plan.field_placeholders() {
            if !dataset.has_field(field) {
                return Err(format!(
                    "{field} not found in dataset fields: {}",
                    dataset.fields.join(",")
                ));
            }
        }
        Ok(Some(dataset))
    }

    pub fn load(path: &str, maxdocs: Option<usize>) -> Result<Self, String> {
        let content = fs::read_to_string(path).map_err(|err| format!("{path}: {err}"))?;
        let delimiter = detect_delimiter(path, &content);
        let records = if delimiter == '\t' {
            parse_tsv(&content)
        } else {
            parse_csv(&content)?
        };
        if records.is_empty() {
            return Err("Dataset is empty".to_string());
        }
        let fields = records[0].clone();
        let index = fields
            .iter()
            .enumerate()
            .map(|(idx, field)| (field.clone(), idx))
            .collect();
        let limit = maxdocs.unwrap_or(usize::MAX);
        let rows = records
            .into_iter()
            .skip(1)
            .filter(|row| row.iter().any(|field| !field.is_empty()))
            .take(limit)
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Err("Dataset has no documents".to_string());
        }
        Ok(Self {
            fields,
            rows,
            index,
        })
    }

    pub fn has_field(&self, field: &str) -> bool {
        self.index.contains_key(field)
    }

    pub fn get(&self, row: usize, field: &str) -> Option<&str> {
        let idx = *self.index.get(field)?;
        self.rows
            .get(row % self.rows.len())
            .and_then(|record| record.get(idx))
            .map(String::as_str)
    }
}

fn parse_tsv(content: &str) -> Vec<Vec<String>> {
    content
        .lines()
        .map(|line| line.split('\t').map(ToString::to_string).collect())
        .collect()
}

fn detect_delimiter(path: &str, content: &str) -> char {
    if path.ends_with(".tsv") {
        return '\t';
    }
    let header = content.lines().next().unwrap_or_default();
    let tabs = header.matches('\t').count();
    let commas = header.matches(',').count();
    if tabs > commas {
        '\t'
    } else {
        ','
    }
}

fn parse_csv(content: &str) -> Result<Vec<Vec<String>>, String> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut chars = content.chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '"' if !in_quotes
                && field.is_empty()
                && matches!(chars.peek(), None | Some('\r' | '\n')) =>
            {
                field.push('"');
            }
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                row.push(std::mem::take(&mut field));
            }
            '\n' if !in_quotes => {
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
            }
            '\r' if !in_quotes => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
            }
            _ => field.push(ch),
        }
    }
    if in_quotes {
        return Err("Unterminated quoted CSV field".to_string());
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parses_quoted_csv_values_containing_commas() {
        let rows = parse_csv("id,title\n1,\"Book, Part 1\"\n").unwrap();
        assert_eq!(rows[1][1], "Book, Part 1");
    }

    #[test]
    fn parses_escaped_quotes() {
        let rows = parse_csv("id,title\n1,\"a \"\"quoted\"\" title\"\n").unwrap();
        assert_eq!(rows[1][1], "a \"quoted\" title");
    }

    #[test]
    fn loads_dataset_with_maxdocs() {
        let path = temp_path("dataset-maxdocs.csv");
        let mut file = fs::File::create(&path).unwrap();
        writeln!(file, "name,value\nitem1,value1\nitem2,value2\nitem3,value3").unwrap();
        let dataset = Dataset::load(path.to_str().unwrap(), Some(2)).unwrap();
        assert_eq!(dataset.rows.len(), 2);
        assert_eq!(dataset.get(2, "value"), Some("value1"));
        let _ = fs::remove_file(path);
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("versal-benchmark-{name}-{}", std::process::id()));
        path
    }
}
