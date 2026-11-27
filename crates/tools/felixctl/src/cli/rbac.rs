//! RBAC policy and grouping commands.

use clap::{Args, Subcommand};

use super::root::Confirm;

#[derive(Debug, Subcommand)]
pub(crate) enum RbacCommand {
    /// Policies: what a subject may do to an object
    #[command(
        subcommand,
        long_about = "List, add and remove policies. Each one is a subject, an object and \
                      an action.\n\n\
                      Objects: cluster:*, node:{node}, tenant:{tenant}, \
                      namespace:{tenant}/{ns}, stream:{tenant}/{ns}/{stream}, \
                      cache:{tenant}/{ns}/{cache}, cache:{tenant}/{ns}/{cache}/{key}, \
                      cache:{tenant}/{ns}/{cache}/{prefix}*, and \
                      group:{tenant}/{ns}/{stream}/{group}. A `*` may stand for a stream, \
                      cache or group, and for the namespace above it when it does.",
        after_long_help = "Examples:
  felixctl rbac policy ls --subject role:reader
  felixctl rbac policy add role:reader stream:t1/payments/* stream.subscribe
  felixctl rbac policy add role:session-1 cache:t1/default/sessions/user-1 cache.read
  felixctl rbac policy add role:users cache:t1/default/sessions/user:* cache.write
  felixctl rbac policy rm role:reader stream:t1/payments/* stream.subscribe"
    )]
    Policy(PolicyCommand),
    /// Groupings: which users and groups hold which roles
    #[command(
        subcommand,
        long_about = "List, add and remove groupings. Each one assigns a user, or a group \
                      named by an IdP claim, to a role.",
        after_long_help = "Examples:
  felixctl rbac grouping ls --role role:reader
  felixctl rbac grouping add p:alice role:reader
  felixctl rbac grouping rm p:alice role:reader"
    )]
    Grouping(GroupingCommand),
}

#[derive(Debug, Subcommand)]
pub(crate) enum PolicyCommand {
    /// List policies
    #[command(
        long_about = "List the tenant's policies that the control-plane token may see.",
        after_long_help = "Examples:
  felixctl rbac policy ls
  felixctl rbac policy ls --subject role:reader --json"
    )]
    Ls {
        /// Only policies for this subject
        #[arg(long, value_name = "SUBJECT")]
        subject: Option<String>,
    },
    /// Add a policy
    #[command(
        long_about = "Let SUBJECT take ACTION on OBJECT. The object is checked here before \
                      it is sent; the control plane checks it again, along with the action \
                      and whether the token may grant it. A cache key or key prefix object \
                      takes only cache.read or cache.write.",
        after_long_help = "Examples:
  felixctl rbac policy add role:writer stream:t1/payments/orders stream.publish
  felixctl rbac policy add role:users cache:t1/default/sessions/user:* cache.read"
    )]
    Add(PolicyArgs),
    /// Remove a policy
    #[command(
        long_about = "Remove one policy, named exactly as `policy ls` prints it. Asks first \
                      on a terminal; elsewhere it needs --yes. Exits with status 5 when \
                      there is no such policy.",
        after_long_help = "Examples:
  felixctl rbac policy rm role:writer stream:t1/payments/orders stream.publish
  felixctl rbac policy rm role:writer stream:t1/payments/orders stream.publish --yes"
    )]
    Rm {
        #[command(flatten)]
        policy: PolicyArgs,
        #[command(flatten)]
        confirm: Confirm,
    },
}

#[derive(Debug, Clone, Args)]
pub(crate) struct PolicyArgs {
    /// Who the policy is for, usually a role such as role:reader
    pub(crate) subject: String,
    /// What it covers, such as stream:t1/payments/orders
    pub(crate) object: String,
    /// What it allows, such as stream.publish
    pub(crate) action: String,
}

#[derive(Debug, Subcommand)]
pub(crate) enum GroupingCommand {
    /// List groupings
    #[command(
        long_about = "List the tenant's groupings for roles the control-plane token may see.",
        after_long_help = "Examples:
  felixctl rbac grouping ls
  felixctl rbac grouping ls --user p:alice --json"
    )]
    Ls {
        /// Only groupings for this user or group
        #[arg(long, value_name = "USER")]
        user: Option<String>,
        /// Only groupings to this role
        #[arg(long, value_name = "ROLE")]
        role: Option<String>,
    },
    /// Assign a role
    #[command(
        long_about = "Assign ROLE to USER. The token must be able to grant every policy \
                      the role carries.",
        after_long_help = "Examples:
  felixctl rbac grouping add p:alice role:reader"
    )]
    Add(GroupingArgs),
    /// Remove a role assignment
    #[command(
        long_about = "Take ROLE away from USER. Asks first on a terminal; elsewhere it \
                      needs --yes. Exits with status 5 when there is no such assignment.",
        after_long_help = "Examples:
  felixctl rbac grouping rm p:alice role:reader --yes"
    )]
    Rm {
        #[command(flatten)]
        grouping: GroupingArgs,
        #[command(flatten)]
        confirm: Confirm,
    },
}

#[derive(Debug, Clone, Args)]
pub(crate) struct GroupingArgs {
    /// The user or group, such as p:alice
    pub(crate) user: String,
    /// The role, such as role:reader
    pub(crate) role: String,
}
