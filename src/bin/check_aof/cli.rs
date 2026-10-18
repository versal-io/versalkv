use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AofCheckMode {
    Check,
    Fix,
    Timestamp(u64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AofCheckOptions {
    pub file: PathBuf,
    pub mode: AofCheckMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AofCliAction {
    Version,
    Check(AofCheckOptions),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AofCliError {
    Usage,
    InvalidTimestamp,
    PathTooLong,
}

pub fn parse_args(args: &[String]) -> Result<AofCliAction, AofCliError> {
    let options = match args {
        [version] if version == "-v" || version == "--version" => {
            return Ok(AofCliAction::Version);
        }
        [file] => AofCheckOptions {
            file: PathBuf::from(file),
            mode: AofCheckMode::Check,
        },
        [fix, file] if fix == "--fix" => AofCheckOptions {
            file: PathBuf::from(file),
            mode: AofCheckMode::Fix,
        },
        [option, timestamp, file] if option == "--truncate-to-timestamp" => {
            let timestamp = timestamp
                .parse()
                .map_err(|_| AofCliError::InvalidTimestamp)?;
            AofCheckOptions {
                file: PathBuf::from(file),
                mode: if timestamp == 0 {
                    AofCheckMode::Check
                } else {
                    AofCheckMode::Timestamp(timestamp)
                },
            }
        }
        _ => return Err(AofCliError::Usage),
    };
    validate_path_len(options)
}

fn validate_path_len(options: AofCheckOptions) -> Result<AofCliAction, AofCliError> {
    if options.file.as_os_str().len() > libc::PATH_MAX as usize {
        Err(AofCliError::PathTooLong)
    } else {
        Ok(AofCliAction::Check(options))
    }
}
