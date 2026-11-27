//! Connection profiles (`context`).

use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum ContextCommand {
    /// Save the connection flags given on this command line as a context
    #[command(
        long_about = "Save the connection flags given on this command line as a new \
                      context. Only flags are saved; environment variables are not. The \
                      first context added becomes the current one.",
        after_long_help = "Examples:
  felixctl context add local --brokers 127.0.0.1:5000 --tenant t1 \\
      --token-file token.jwt --ca-file broker-cert.pem
  felixctl context add prod --brokers a.example:5000,b.example:5000 \\
      --controlplane-url https://cp.example --tenant acme --use"
    )]
    Add {
        /// Name of the context
        name: String,
        /// Make it the current context
        #[arg(long = "use")]
        make_current: bool,
        /// Replace a context that already has this name
        #[arg(long)]
        replace: bool,
    },
    /// Make a context the current one
    #[command(
        long_about = "Make NAME the context every command uses unless --context or \
                      FELIX_CONTEXT says otherwise.",
        after_long_help = "Examples:
  felixctl context use prod"
    )]
    Use {
        /// Name of the context
        name: String,
    },
    /// List contexts
    #[command(
        long_about = "List the saved contexts. The current one is marked with *.",
        after_long_help = "Examples:
  felixctl context ls
  felixctl context ls --json"
    )]
    Ls,
    /// Delete a context
    #[command(
        long_about = "Delete a saved context. Deleting the current one leaves no context \
                      current.",
        after_long_help = "Examples:
  felixctl context rm local"
    )]
    Rm {
        /// Name of the context
        name: String,
    },
}
