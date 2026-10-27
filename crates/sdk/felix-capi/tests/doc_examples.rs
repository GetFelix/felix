//! Every ```c block on the C client page and in the tutorials compiles against
//! `include/felix.h`, so the examples cannot drift from the ABI.
//!
//! A block with `int main(` is a program, compiled and linked against the
//! shared library. Any other block is a fragment, compiled to an object; one
//! without `#include`s of its own gets `tests/c/doc_prelude.h` first. Blocks
//! that include `<curl/curl.h>` are skipped where that header is missing,
//! unless `FELIX_CAPI_DOCS_REQUIRE_CURL` is set, as CI's C ABI step does.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;

const CFLAGS: &[&str] = &["-std=c11", "-pthread", "-Wall", "-Wextra", "-Werror"];

/// The page every tutorial program is checked beside, and the tutorials that
/// must keep a C program.
const C_PAGE: &str = "clients/c.md";
/// The resilience guide has its own prelude, with the waiter helpers its
/// fragments call.
const RESILIENT_PAGE: &str = "clients/resilient-clients.mdx";
const TUTORIALS_WITH_C: &[&str] = &[
    "tutorials/first-stream.mdx",
    "tutorials/work-queue.mdx",
    "tutorials/cache-leases-watches.mdx",
];

/// The C page, then every tutorial page. A tutorial may have no C block.
fn pages(docs: &Path) -> Vec<String> {
    let mut pages = vec![C_PAGE.to_string(), RESILIENT_PAGE.to_string()];
    let mut tutorials: Vec<String> = std::fs::read_dir(docs.join("tutorials"))
        .expect("read the tutorials directory")
        .filter_map(|entry| {
            let name = entry.expect("read a directory entry").file_name();
            let name = name.to_str()?;
            name.ends_with(".mdx").then(|| format!("tutorials/{name}"))
        })
        .collect();
    tutorials.sort();
    pages.extend(tutorials);
    pages
}

struct Block {
    line: usize,
    code: String,
}

fn c_blocks(page: &str) -> Vec<Block> {
    let mut blocks = Vec::new();
    let mut open: Option<Block> = None;
    for (index, line) in page.lines().enumerate() {
        match open.as_mut() {
            None if line.trim_end() == "```c" => {
                open = Some(Block {
                    line: index + 2,
                    code: String::new(),
                });
            }
            Some(_) if line.trim_end() == "```" => blocks.extend(open.take()),
            Some(block) => {
                block.code.push_str(line);
                block.code.push('\n');
            }
            None => {}
        }
    }
    assert!(open.is_none(), "a ```c block on the page is never closed");
    blocks
}

fn has_curl_header() -> bool {
    Command::new("cc")
        .args(["-E", "-x", "c", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .and_then(|mut cc| {
            use std::io::Write;
            cc.stdin
                .take()
                .expect("piped stdin")
                .write_all(b"#include <curl/curl.h>\n")?;
            cc.wait()
        })
        .is_ok_and(|status| status.success())
}

/// The shared library cargo built for this test run. It sits next to the
/// test binary in `deps`, and is copied up a level by a plain `cargo build`.
fn shared_library() -> PathBuf {
    let name = if cfg!(target_os = "macos") {
        "libfelix.dylib"
    } else {
        "libfelix.so"
    };
    let exe = std::env::current_exe().expect("test binary path");
    let deps = exe.parent().expect("the test binary has a directory");
    std::iter::successors(Some(deps), |dir| dir.parent())
        .take(2)
        .map(|dir| dir.join(name))
        .find(|path| path.exists())
        .unwrap_or_else(|| panic!("{name} was not built next to {}", exe.display()))
}

#[test]
fn every_c_example_on_the_docs_pages_compiles() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let docs = crate_dir.join("../../../docs-site/src/content/docs");
    let include = crate_dir.join("include");
    let prelude = crate_dir.join("tests/c/doc_prelude.h");
    let resilient_prelude = crate_dir.join("tests/c/resilient_prelude.h");
    let shared = shared_library();
    let work = Path::new(env!("CARGO_TARGET_TMPDIR")).join("capi-doc-examples");
    std::fs::create_dir_all(&work).expect("create the work directory");
    let curl = has_curl_header();
    let require_curl = std::env::var_os("FELIX_CAPI_DOCS_REQUIRE_CURL").is_some();

    let mut failures = Vec::new();
    for page_name in &pages(&docs) {
        let page = std::fs::read_to_string(docs.join(page_name))
            .unwrap_or_else(|err| panic!("read {page_name}: {err}"));
        let blocks = c_blocks(&page);
        assert!(
            !(page_name == C_PAGE
                || page_name == RESILIENT_PAGE
                || TUTORIALS_WITH_C.contains(&page_name.as_str()))
                || !blocks.is_empty(),
            "no ```c blocks found on {page_name}"
        );
        let stem = page_name.replace(['/', '.'], "_");

        for block in &blocks {
            let at = format!("{page_name} line {}", block.line);
            if block.code.contains("#include <curl/curl.h>") && !curl {
                assert!(
                    !require_curl,
                    "{at} needs libcurl's header, which is missing"
                );
                eprintln!("skipping {at}: libcurl's header is not installed");
                continue;
            }
            let name = format!("{stem}_line{}", block.line);
            let source = work.join(format!("{name}.c"));
            std::fs::write(&source, &block.code).expect("write the block");
            let mut cc = Command::new("cc");
            cc.args(CFLAGS).arg("-I").arg(&include);
            if block.code.contains("int main(") {
                cc.arg(&source)
                    .arg(&shared)
                    .arg(format!(
                        "-Wl,-rpath,{}",
                        shared.parent().expect("library directory").display()
                    ))
                    .arg("-o")
                    .arg(work.join(&name));
            } else {
                if !block.code.contains("#include") {
                    let prelude = if page_name == RESILIENT_PAGE {
                        &resilient_prelude
                    } else {
                        &prelude
                    };
                    cc.arg("-include").arg(prelude);
                }
                // A fragment's function has no caller.
                cc.args(["-Wno-unused-function", "-c"])
                    .arg(&source)
                    .arg("-o")
                    .arg(work.join(format!("{name}.o")));
            }
            let output = cc.output().expect("run cc");
            if !output.status.success() {
                failures.push(format!(
                    "{at}:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "C examples that do not compile:\n{}",
        failures.join("\n")
    );
}
