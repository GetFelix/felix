//! Tenant and namespace commands.

use clap::Subcommand;

use super::root::Confirm;

#[derive(Debug, Subcommand)]
pub(crate) enum TenantCommand {
    /// List tenants
    #[command(
        long_about = "List every tenant.",
        after_long_help = "Examples:
  felixctl tenant ls
  felixctl tenant ls --json"
    )]
    Ls,
    /// Show one tenant
    #[command(
        long_about = "Show one tenant. Exits with status 5 when it does not exist.",
        after_long_help = "Examples:
  felixctl tenant info t1"
    )]
    Info {
        // Its own id: one named `tenant` would replace the global --tenant here.
        /// Tenant id
        #[arg(id = "tenant_id", value_name = "TENANT")]
        tenant: String,
    },
    /// Create a tenant
    #[command(
        long_about = "Create a tenant. One that already exists is refused.",
        after_long_help = "Examples:
  felixctl tenant create acme
  felixctl tenant create acme --display-name 'Acme Corp'"
    )]
    Create {
        /// Tenant id
        #[arg(id = "tenant_id", value_name = "TENANT")]
        tenant: String,
        /// Display name [default: the tenant id]
        #[arg(long, value_name = "TEXT")]
        display_name: Option<String>,
    },
    /// Delete a tenant
    #[command(
        long_about = "Delete a tenant with everything in it: namespaces, streams, caches, \
                      signing keys and RBAC rules. Asks first on a terminal; elsewhere it \
                      needs --yes.",
        after_long_help = "Examples:
  felixctl tenant rm acme
  felixctl tenant rm acme --yes"
    )]
    Rm {
        /// Tenant id
        #[arg(id = "tenant_id", value_name = "TENANT")]
        tenant: String,
        #[command(flatten)]
        confirm: Confirm,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum NamespaceCommand {
    /// List namespaces in the tenant
    #[command(
        long_about = "List the namespaces of the current tenant.",
        after_long_help = "Examples:
  felixctl namespace ls
  felixctl namespace ls --tenant acme"
    )]
    Ls,
    /// Show one namespace
    #[command(
        long_about = "Show one namespace of the current tenant.",
        after_long_help = "Examples:
  felixctl namespace info default"
    )]
    Info {
        // Its own id: one named `namespace` would replace the global --namespace.
        /// Namespace name
        #[arg(id = "namespace_name", value_name = "NAMESPACE")]
        namespace: String,
    },
    /// Create a namespace
    #[command(
        long_about = "Create a namespace in the current tenant. One that already exists is \
                      refused.",
        after_long_help = "Examples:
  felixctl namespace create payments
  felixctl namespace create payments --display-name Payments"
    )]
    Create {
        /// Namespace name
        #[arg(id = "namespace_name", value_name = "NAMESPACE")]
        namespace: String,
        /// Display name [default: the namespace name]
        #[arg(long, value_name = "TEXT")]
        display_name: Option<String>,
    },
    /// Delete a namespace
    #[command(
        long_about = "Delete a namespace of the current tenant, its streams and caches \
                      included. Asks first on a terminal; elsewhere it needs --yes.",
        after_long_help = "Examples:
  felixctl namespace rm payments
  felixctl namespace rm payments --yes"
    )]
    Rm {
        /// Namespace name
        #[arg(id = "namespace_name", value_name = "NAMESPACE")]
        namespace: String,
        #[command(flatten)]
        confirm: Confirm,
    },
}
