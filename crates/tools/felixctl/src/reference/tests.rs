use std::path::PathBuf;

use super::*;

fn page() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../docs-site/src/content/docs/reference/felixctl.md")
}

/// The committed page is what the binary renders. Regenerate with `task docs:felixctl`.
#[test]
fn the_reference_page_matches_the_command_definitions() {
    let rendered = markdown();
    let committed = std::fs::read_to_string(page()).unwrap_or_default();
    assert!(
        rendered == committed,
        "docs-site/src/content/docs/reference/felixctl.md is out of date; run `task docs:felixctl`"
    );
}

#[test]
fn every_visible_command_has_a_section() {
    let rendered = markdown();
    let mut root = Cli::command();
    root.build();
    fn walk(command: &Command, name: &str, rendered: &str) {
        for sub in visible_subcommands(command) {
            let path = format!("{name} {}", sub.get_name());
            assert!(
                rendered.contains(&format!("`{path}`\n")),
                "no section for `{path}`"
            );
            walk(sub, &path, rendered);
        }
    }
    walk(&root, "felixctl", &rendered);
}

#[test]
fn hidden_commands_are_left_out() {
    let rendered = markdown();
    assert!(!rendered.contains("felixctl man"));
    assert!(!rendered.contains("felixctl reference"));
}

#[test]
fn placeholders_are_not_read_as_html() {
    assert_eq!(
        prose("sub <STREAM> `a|b` x|y"),
        "sub \\<STREAM> `a\\|b` x\\|y"
    );
}
