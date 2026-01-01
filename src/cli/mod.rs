//! CLI argument parsing and file discovery

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// sqlflight - An opinionated SQL formatter for Snowflake
#[derive(Parser, Debug)]
#[command(name = "sqlflight")]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Format SQL files
    Fmt {
        /// Write formatted output back to files
        #[arg(short, long)]
        write: bool,

        /// Files or directories to format (use - for stdin)
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
    /// Check if files are formatted
    Check {
        /// Files or directories to check
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
}

/// Discover SQL files in the given paths
pub fn discover_files(paths: &[PathBuf]) -> crate::Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for path in paths {
        if path.is_file() {
            files.push(path.clone());
        } else if path.is_dir() {
            for entry in walkdir::WalkDir::new(path)
                .follow_links(true)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                let path = entry.path();
                if path.is_file() && path.extension().map_or(false, |ext| ext == "sql") {
                    files.push(path.to_path_buf());
                }
            }
        }
    }

    Ok(files)
}
