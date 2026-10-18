use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RdbOutputFormat {
    Table,
    Info,
    Csv,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RdbCheckOptions {
    pub file: PathBuf,
    pub show_stats: bool,
    pub format: RdbOutputFormat,
    pub output_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RdbCliAction {
    Version,
    Check(RdbCheckOptions),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RdbCliError {
    Usage,
}

pub fn parse_args(args: &[String]) -> Result<RdbCliAction, RdbCliError> {
    if matches!(args.first().map(String::as_str), Some("-v" | "--version")) {
        return Ok(RdbCliAction::Version);
    }
    let mut parser = OptionParser::new(args);
    parser.parse()
}

struct OptionParser<'a> {
    args: &'a [String],
    index: usize,
    file: Option<PathBuf>,
    show_stats: bool,
    format: RdbOutputFormat,
    output_file: Option<PathBuf>,
}

impl<'a> OptionParser<'a> {
    fn new(args: &'a [String]) -> Self {
        Self {
            args,
            index: 0,
            file: None,
            show_stats: false,
            format: RdbOutputFormat::Table,
            output_file: None,
        }
    }

    fn parse(&mut self) -> Result<RdbCliAction, RdbCliError> {
        while let Some(argument) = self.args.get(self.index) {
            match argument.as_str() {
                "--stats" => self.show_stats = true,
                "--format" => self.parse_format()?,
                "--output" => self.parse_output()?,
                _ if self.file.is_none() => self.file = Some(PathBuf::from(argument)),
                _ => return Err(RdbCliError::Usage),
            }
            self.index += 1;
        }
        let file = self.file.take().ok_or(RdbCliError::Usage)?;
        Ok(RdbCliAction::Check(RdbCheckOptions {
            file,
            show_stats: self.show_stats,
            format: self.format,
            output_file: self.output_file.take(),
        }))
    }

    fn parse_format(&mut self) -> Result<(), RdbCliError> {
        self.index += 1;
        self.format = match self.args.get(self.index).map(String::as_str) {
            Some("table") => RdbOutputFormat::Table,
            Some("info") => RdbOutputFormat::Info,
            Some("csv") => RdbOutputFormat::Csv,
            _ => return Err(RdbCliError::Usage),
        };
        Ok(())
    }

    fn parse_output(&mut self) -> Result<(), RdbCliError> {
        self.index += 1;
        let value = self.args.get(self.index).ok_or(RdbCliError::Usage)?;
        self.output_file = Some(PathBuf::from(value));
        Ok(())
    }
}
