use std::fs;
use std::process::Command;

use crate::application::{PortError, ThreeWayMergePort};
use crate::domain::MergeOutcome;

#[derive(Clone, Copy, Default)]
pub struct GitThreeWayMerge;

impl ThreeWayMergePort for GitThreeWayMerge {
    fn available(&self) -> Result<bool, PortError> {
        Ok(Command::new("git")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false))
    }

    fn merge(&self, base: &[u8], local: &[u8], upstream: &[u8]) -> Result<MergeOutcome, PortError> {
        let temp = tempfile::tempdir().map_err(io_error)?;
        let local_path = temp.path().join("local");
        let base_path = temp.path().join("base");
        let upstream_path = temp.path().join("upstream");
        fs::write(&local_path, local).map_err(io_error)?;
        fs::write(&base_path, base).map_err(io_error)?;
        fs::write(&upstream_path, upstream).map_err(io_error)?;
        let output = Command::new("git")
            .args([
                "merge-file",
                "-p",
                "--diff3",
                "-L",
                "LOCAL",
                "-L",
                "BASE",
                "-L",
                "UPSTREAM",
            ])
            .arg(&local_path)
            .arg(&base_path)
            .arg(&upstream_path)
            .output()
            .map_err(|error| PortError::new(format!("could not run git merge-file: {error}")))?;
        match output.status.code() {
            Some(0) => Ok(MergeOutcome::Clean(output.stdout)),
            Some(code) if (1..=127).contains(&code) => Ok(MergeOutcome::Conflict {
                content: output.stdout,
                detail: "local and upstream changes overlap".to_owned(),
            }),
            _ => Err(PortError::new(format!(
                "git merge-file failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ))),
        }
    }
}

fn io_error(error: std::io::Error) -> PortError {
    PortError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_merges_non_overlapping_changes_and_reports_overlap() {
        let merger = GitThreeWayMerge;
        let clean = merger
            .merge(
                b"one\ntwo\nthree\n",
                b"ONE\ntwo\nthree\n",
                b"one\ntwo\nTHREE\n",
            )
            .unwrap();
        assert!(matches!(clean, MergeOutcome::Clean(_)));
        let conflict = merger.merge(b"one\n", b"local\n", b"upstream\n").unwrap();
        assert!(matches!(conflict, MergeOutcome::Conflict { .. }));
    }

    #[test]
    fn git_reports_conflicts_when_multiple_hunks_overlap() {
        let merger = GitThreeWayMerge;
        let base = b"one\ntwo\nthree\nfour\nfive\nsix\n";
        let local = b"one\nLOCAL\nthree\nfour\nLOCAL-TWO\nsix\n";
        let upstream = b"one\nUPSTREAM\nthree\nfour\nUPSTREAM-TWO\nsix\n";
        let conflict = merger.merge(base, local, upstream).unwrap();
        match conflict {
            MergeOutcome::Conflict { content, detail } => {
                assert!(
                    content.windows(7).any(|window| window == b"<<<<<<<"),
                    "expected at least one conflict marker in stdout, got: {}",
                    String::from_utf8_lossy(&content)
                );
                assert!(
                    content.windows(7).any(|window| window == b">>>>>>>"),
                    "expected closing conflict markers, got: {}",
                    String::from_utf8_lossy(&content)
                );
                assert!(detail.contains("overlap"));
            }
            other => panic!("expected Conflict, got {:?}", other),
        }
    }
}
