//! The shared workspace-directory CLI argument (issue #128).
//!
//! Apps embed this with `#[command(flatten)]`. The flag is uniform
//! (`--workspace-dir`); the per-app historical names are accepted as clap
//! aliases for one deprecation cycle. Env resolution stays in the caller —
//! pass this value to `Workspace::resolve`, which applies the per-app
//! `SAPPHIRE_<APP>_DIR` fallback (and the deprecated `SAPPHIRE_WORKSPACE_DIR`).

use std::path::PathBuf;

/// Shared `--workspace-dir` argument: the explicit workspace root, overriding
/// the upward marker search.
#[derive(clap::Args)]
pub struct WorkspaceArgs {
    /// Workspace root directory. Overrides the automatic upward search.
    #[arg(
        long = "workspace-dir",
        alias = "journal-dir",
        alias = "ledger-dir",
        alias = "data-dir",
        global = true,
        value_name = "DIR"
    )]
    pub workspace_dir: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Probe {
        #[command(flatten)]
        args: WorkspaceArgs,
    }

    #[test]
    fn canonical_flag_and_legacy_aliases_all_parse() {
        for flag in [
            "--workspace-dir",
            "--journal-dir",
            "--ledger-dir",
            "--data-dir",
        ] {
            let parsed = Probe::try_parse_from(["app", flag, "/tmp/x"]).unwrap();
            assert_eq!(
                parsed.args.workspace_dir.unwrap(),
                std::path::PathBuf::from("/tmp/x"),
                "{flag}"
            );
        }
    }

    #[derive(Parser)]
    struct Sub {
        #[command(flatten)]
        args: WorkspaceArgs,
    }

    #[derive(clap::Subcommand)]
    enum Cmd {
        Sub(Box<Sub>),
    }

    #[derive(Parser)]
    struct App {
        #[command(subcommand)]
        cmd: Option<Cmd>,
    }

    #[test]
    fn global_flag_parses_after_a_subcommand() {
        let app = App::try_parse_from(["app", "sub", "--workspace-dir", "/tmp/y"]).unwrap();
        match app.cmd {
            Some(Cmd::Sub(sub)) => {
                assert_eq!(
                    sub.args.workspace_dir.unwrap(),
                    std::path::PathBuf::from("/tmp/y")
                )
            }
            None => panic!("subcommand was not parsed"),
        }
    }

    #[test]
    fn absent_flag_is_none() {
        assert!(
            Probe::try_parse_from(["app"])
                .unwrap()
                .args
                .workspace_dir
                .is_none()
        );
    }
}
