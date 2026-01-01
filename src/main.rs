//! sqlflight CLI - An opinionated SQL formatter for Snowflake

use clap::{Parser, Subcommand};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use walkdir::WalkDir;

/// An opinionated SQL formatter for Snowflake with first-class Jinja support
#[derive(Parser)]
#[command(name = "sqlflight")]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Format SQL files
    Fmt {
        /// Write formatted output back to files
        #[arg(short, long)]
        write: bool,

        /// Files or directories to format (use - for stdin)
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
    /// Check if files are formatted (exit code 1 if not)
    Check {
        /// Files or directories to check
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Commands::Fmt { write, files } => run_fmt(&files, write),
        Commands::Check { files } => run_check(&files),
    }
}

/// Run the fmt command
fn run_fmt(files: &[PathBuf], write_mode: bool) -> ExitCode {
    let mut had_errors = false;

    for file_path in files {
        // Handle stdin
        if file_path == Path::new("-") {
            match format_stdin() {
                Ok(formatted) => {
                    print!("{}", formatted);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    had_errors = true;
                }
            }
            continue;
        }

        // Handle files and directories
        for entry in discover_sql_files(file_path) {
            match format_file(&entry, write_mode) {
                Ok(changed) => {
                    if changed && !write_mode {
                        // In non-write mode, print the formatted output
                        // (already printed in format_file)
                    }
                }
                Err(e) => {
                    eprintln!("{}: {}", entry.display(), e);
                    had_errors = true;
                }
            }
        }
    }

    if had_errors {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    }
}

/// Run the check command
fn run_check(files: &[PathBuf]) -> ExitCode {
    let mut needs_formatting = false;
    let mut had_errors = false;

    for file_path in files {
        // Handle stdin
        if file_path == Path::new("-") {
            match check_stdin() {
                Ok(formatted) => {
                    if !formatted {
                        eprintln!("<stdin>: needs formatting");
                        needs_formatting = true;
                    }
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    had_errors = true;
                }
            }
            continue;
        }

        // Handle files and directories
        for entry in discover_sql_files(file_path) {
            match check_file(&entry) {
                Ok(formatted) => {
                    if !formatted {
                        eprintln!("{}: needs formatting", entry.display());
                        needs_formatting = true;
                    }
                }
                Err(e) => {
                    eprintln!("{}: {}", entry.display(), e);
                    had_errors = true;
                }
            }
        }
    }

    if had_errors {
        ExitCode::from(2)
    } else if needs_formatting {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

/// Discover SQL files from a path (file or directory)
fn discover_sql_files(path: &Path) -> Vec<PathBuf> {
    if path.is_file() {
        return vec![path.to_path_buf()];
    }

    if path.is_dir() {
        let mut files = Vec::new();
        for entry in WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "sql" {
                    files.push(path.to_path_buf());
                }
            }
        }
        return files;
    }

    // Handle glob patterns
    if let Ok(paths) = glob::glob(path.to_str().unwrap_or("")) {
        return paths
            .filter_map(|p| p.ok())
            .filter(|p| p.is_file())
            .filter(|p| p.extension().map_or(false, |e| e == "sql"))
            .collect();
    }

    vec![]
}

/// Format a single file
fn format_file(path: &Path, write_mode: bool) -> Result<bool, sqlflight::Error> {
    let contents = fs::read_to_string(path)
        .map_err(|e| sqlflight::Error::FormatError {
            message: format!("Failed to read file: {}", e),
        })?;

    let formatted = sqlflight::format(&contents)?;

    if formatted == contents {
        return Ok(false);
    }

    if write_mode {
        fs::write(path, &formatted)
            .map_err(|e| sqlflight::Error::FormatError {
                message: format!("Failed to write file: {}", e),
            })?;
    } else {
        print!("{}", formatted);
    }

    Ok(true)
}

/// Check a single file
fn check_file(path: &Path) -> Result<bool, sqlflight::Error> {
    let contents = fs::read_to_string(path)
        .map_err(|e| sqlflight::Error::FormatError {
            message: format!("Failed to read file: {}", e),
        })?;

    sqlflight::check(&contents)
}

/// Format from stdin
fn format_stdin() -> Result<String, sqlflight::Error> {
    let mut contents = String::new();
    io::stdin()
        .read_to_string(&mut contents)
        .map_err(|e| sqlflight::Error::FormatError {
            message: format!("Failed to read stdin: {}", e),
        })?;

    sqlflight::format(&contents)
}

/// Check stdin
fn check_stdin() -> Result<bool, sqlflight::Error> {
    let mut contents = String::new();
    io::stdin()
        .read_to_string(&mut contents)
        .map_err(|e| sqlflight::Error::FormatError {
            message: format!("Failed to read stdin: {}", e),
        })?;

    sqlflight::check(&contents)
}
