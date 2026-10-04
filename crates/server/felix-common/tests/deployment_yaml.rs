//! The YAML in the docs is real YAML that names real things.
//!
//! The deployment pages carry hundreds of lines of Kubernetes and Compose
//! YAML that nothing loaded. One block did not parse at all and the page
//! looked fine; a `build.args` key no Dockerfile declared was ignored by
//! Docker with a warning nobody reads. Both failed silently, which is the
//! failure mode a docs page cannot afford.
//!
//! A Rust test rather than a docs-site script: the workspace already has a
//! YAML parser, so this needs no new dependency, and it runs in the same CI
//! job as the code the pages describe. It lives in `felix-common` because
//! that is where the environment-variable registry is, and the registry is
//! what the `FELIX_*` names in the YAML are checked against.
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_yaml_ng::Value;

/// The only two images a release publishes.
const PUBLISHED_IMAGES: &[&str] = &["felix-broker", "felix-controlplane"];

/// Where releases publish. 0.6.0-preview and earlier went out under gabloe,
/// and the docs still pin it there.
const IMAGE_OWNERS: &[&str] = &["ghcr.io/getfelix/", "ghcr.io/gabloe/"];

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

/// Every markdown page under the two documentation roots.
fn pages() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for root in ["docs", "docs-site/src/content/docs"] {
        walk(&repo().join(root), &mut out);
    }
    out.sort();
    assert!(!out.is_empty(), "no documentation pages found");
    out
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|err| panic!("{}: {err}", dir.display())) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            walk(&path, out);
        } else if path
            .extension()
            .is_some_and(|ext| ext == "md" || ext == "mdx")
        {
            out.push(path);
        }
    }
}

/// A fenced block: where it starts and what it holds.
struct Block {
    page: PathBuf,
    line: usize,
    text: String,
}

impl Block {
    fn at(&self) -> String {
        format!(
            "{}:{}",
            self.page
                .strip_prefix(repo())
                .unwrap_or(&self.page)
                .display(),
            self.line
        )
    }
}

/// Fenced blocks whose info string is one of `langs`.
fn fenced(langs: &[&str]) -> Vec<Block> {
    let mut blocks = Vec::new();
    for page in pages() {
        let text = std::fs::read_to_string(&page).expect("read page");
        // `Some((start, Some(body)))` inside a wanted fence, `Some((_, None))`
        // inside some other fence, whose contents are skipped so a ```yaml
        // shown inside a ```markdown example is not picked up.
        let mut open: Option<(usize, Option<String>)> = None;
        for (index, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            match &mut open {
                None => {
                    if let Some(info) = trimmed.strip_prefix("```") {
                        let lang = info.split_whitespace().next().unwrap_or("");
                        open = Some((index + 1, langs.contains(&lang).then(String::new)));
                    }
                }
                Some((start, body)) => {
                    if trimmed.starts_with("```") {
                        if let Some(body) = body.take() {
                            blocks.push(Block {
                                page: page.clone(),
                                line: *start,
                                text: body,
                            });
                        }
                        open = None;
                    } else if let Some(body) = body {
                        body.push_str(line);
                        body.push('\n');
                    }
                }
            }
        }
        assert!(open.is_none(), "{}: an unclosed code fence", page.display());
    }
    blocks
}

/// Every document in a block; a Kubernetes page separates several with `---`.
fn documents(block: &Block) -> Result<Vec<Value>, String> {
    serde_yaml_ng::Deserializer::from_str(&block.text)
        .map(|doc| Value::deserialize(doc).map_err(|err| err.to_string()))
        .collect()
}

fn dockerfile_args(path: &Path) -> BTreeSet<String> {
    std::fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("{}: {err}", path.display()))
        .lines()
        .filter_map(|line| line.trim().strip_prefix("ARG "))
        .map(|rest| {
            rest.trim()
                .split(['=', ' '])
                .next()
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

fn every_dockerfile_arg() -> BTreeSet<String> {
    let mut args = BTreeSet::new();
    for entry in std::fs::read_dir(repo().join("docker")).expect("docker/") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|ext| ext == "Dockerfile")
            || path.file_name().is_some_and(|name| name == "Dockerfile")
        {
            args.extend(dockerfile_args(&path));
        }
    }
    assert!(!args.is_empty(), "no ARGs found under docker/");
    args
}

/// Depth-first over every value, with the key it hangs under.
fn visit<'a>(value: &'a Value, key: Option<&'a str>, f: &mut dyn FnMut(Option<&str>, &Value)) {
    f(key, value);
    match value {
        Value::Mapping(map) => {
            for (k, v) in map {
                visit(v, k.as_str(), f);
            }
        }
        Value::Sequence(items) => {
            for item in items {
                visit(item, key, f);
            }
        }
        _ => {}
    }
}

fn is_known_var(name: &str) -> bool {
    felix_common::env_registry::KNOWN_VARS.contains(&name)
}

#[test]
fn every_yaml_block_in_the_docs_parses() {
    let mut failures = Vec::new();
    for block in fenced(&["yaml", "yml"]) {
        if let Err(err) = documents(&block) {
            failures.push(format!("{}: {err}", block.at()));
        }
    }
    assert!(
        failures.is_empty(),
        "YAML blocks that do not load:\n{}",
        failures.join("\n")
    );
}

/// A `build.args` key the Dockerfile does not declare is ignored by Docker
/// with a warning, which is a page that quietly builds something other than
/// what it shows. The same for `--build-arg` on a command line.
#[test]
fn build_args_in_the_docs_are_declared_by_a_dockerfile() {
    let mut failures = Vec::new();

    for block in fenced(&["yaml", "yml"]) {
        let Ok(docs) = documents(&block) else {
            continue;
        };
        for doc in docs {
            let Some(services) = doc.get("services").and_then(Value::as_mapping) else {
                continue;
            };
            for (name, service) in services {
                let Some(build) = service.get("build").and_then(Value::as_mapping) else {
                    continue;
                };
                let name = name.as_str().unwrap_or("?");
                let context = build.get("context").and_then(Value::as_str).unwrap_or(".");
                let dockerfile = build
                    .get("dockerfile")
                    .and_then(Value::as_str)
                    .unwrap_or("Dockerfile");
                let path = repo().join(context).join(dockerfile);
                if !path.is_file() {
                    failures.push(format!(
                        "{}: service {name} builds from {}, which does not exist",
                        block.at(),
                        path.strip_prefix(repo()).unwrap_or(&path).display()
                    ));
                    continue;
                }
                let declared = dockerfile_args(&path);
                let args: Vec<String> = match build.get("args") {
                    Some(Value::Mapping(map)) => map
                        .keys()
                        .filter_map(|k| k.as_str().map(String::from))
                        .collect(),
                    Some(Value::Sequence(items)) => items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(|item| item.split('=').next().unwrap_or_default().to_string())
                        .collect(),
                    _ => Vec::new(),
                };
                for arg in args {
                    if !declared.contains(&arg) {
                        failures.push(format!(
                            "{}: service {name} passes build arg {arg}, which {dockerfile} does not declare",
                            block.at(),
                        ));
                    }
                }
            }
        }
    }

    let declared = every_dockerfile_arg();
    for block in fenced(&["bash", "sh", "shell", "console"]) {
        for (offset, line) in block.text.lines().enumerate() {
            let mut rest = line;
            while let Some(index) = rest.find("--build-arg") {
                rest = &rest[index + "--build-arg".len()..];
                let arg: String = rest
                    .trim_start_matches(['=', ' '])
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                if !arg.is_empty() && !declared.contains(&arg) {
                    failures.push(format!(
                        "{}:{}: --build-arg {arg} is not an ARG in any Dockerfile under docker/",
                        block
                            .page
                            .strip_prefix(repo())
                            .unwrap_or(&block.page)
                            .display(),
                        block.line + offset
                    ));
                }
            }
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Only two images exist. A page naming a third sends someone to a registry
/// that will answer "not found".
#[test]
fn felix_images_in_the_docs_are_the_published_ones() {
    let mut failures = Vec::new();
    for block in fenced(&["yaml", "yml"]) {
        let Ok(docs) = documents(&block) else {
            continue;
        };
        for doc in docs {
            visit(&doc, None, &mut |key, value| {
                if key != Some("image") {
                    return;
                }
                let Some(image) = value.as_str() else {
                    return;
                };
                let Some(rest) = IMAGE_OWNERS.iter().find_map(|o| image.strip_prefix(o)) else {
                    return;
                };
                let name = rest.split([':', '@']).next().unwrap_or(rest);
                if !PUBLISHED_IMAGES.contains(&name) {
                    failures.push(format!("{}: {image} is not a published image", block.at()));
                }
            });
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A `FELIX_*` name in a manifest that no binary reads is a setting that
/// silently takes its default. The registry is what the binaries themselves
/// warn from at startup, so the docs are held to the same list.
#[test]
fn felix_env_vars_in_the_docs_yaml_are_ones_the_binaries_read() {
    let mut failures = Vec::new();
    for block in fenced(&["yaml", "yml"]) {
        let Ok(docs) = documents(&block) else {
            continue;
        };
        for doc in docs {
            let at = block.at();
            let mut check = |name: &str| {
                if name.starts_with("FELIX_") && !is_known_var(name) {
                    let hint = felix_common::env_registry::suggestions(name);
                    failures.push(format!(
                        "{at}: {name} is not read by any binary{}",
                        if hint.is_empty() {
                            String::new()
                        } else {
                            format!(" (did you mean {}?)", hint.join(", "))
                        }
                    ));
                }
            };
            visit(&doc, None, &mut |key, value| match (key, value) {
                // Compose: `environment:` as a mapping or as `KEY=value` items.
                (Some("environment"), Value::Mapping(map)) => {
                    for k in map.keys() {
                        if let Some(name) = k.as_str() {
                            check(name);
                        }
                    }
                }
                (Some("environment"), Value::String(item)) => {
                    check(item.split('=').next().unwrap_or_default());
                }
                // Kubernetes: `env: - name: X`, and ConfigMap `data:` keys.
                (Some("env"), Value::Mapping(map)) => {
                    if let Some(name) = map.get("name").and_then(Value::as_str) {
                        check(name);
                    }
                }
                (Some("data"), Value::Mapping(map)) => {
                    for k in map.keys() {
                        if let Some(name) = k.as_str() {
                            check(name);
                        }
                    }
                }
                _ => {}
            });
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
