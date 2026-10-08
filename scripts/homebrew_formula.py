#!/usr/bin/env python3
"""Print the Homebrew formula for one felixctl release.

release.yml runs this over the release's felixctl archives and pushes the
result to the owner's homebrew-tap repository. The checksums come from the
`.sha256` files the packaging step wrote beside each archive, so the formula
is never edited by hand.

usage: homebrew_formula.py <tag> <dist-dir> [<owner/repo>]
"""

import sys
from pathlib import Path

# (formula block, Rust target). Windows has no Homebrew.
PLATFORMS = [
    ("on_macos", "on_arm", "aarch64-apple-darwin"),
    ("on_macos", "on_intel", "x86_64-apple-darwin"),
    ("on_linux", "on_arm", "aarch64-unknown-linux-gnu"),
    ("on_linux", "on_intel", "x86_64-unknown-linux-gnu"),
]

TEMPLATE = """\
class Felixctl < Formula
  desc "Command-line tool for the Felix streaming and cache broker"
  homepage "https://docs.getfelix.dev/getting-started/felixctl/"
  version "{version}"
  license "AGPL-3.0-only"

{platforms}
  def install
    bin.install "felixctl"
    man1.install Dir["man/*.1"]
    bash_completion.install "completions/felixctl.bash" => "felixctl"
    zsh_completion.install "completions/_felixctl"
    fish_completion.install "completions/felixctl.fish"
  end

  test do
    assert_match version.to_s, shell_output("#{{bin}}/felixctl --version")
  end
end
"""


def checksum(dist: Path, archive: str) -> str:
    path = dist / f"{archive}.sha256"
    if not path.is_file():
        raise SystemExit(f"missing {path}")
    digest = path.read_text().split()[0]
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise SystemExit(f"{path} does not hold a sha256 digest")
    return digest


def formula(tag: str, dist: Path, repo: str) -> str:
    base = f"https://github.com/{repo}/releases/download/{tag}"
    blocks = []
    for os_block in ("on_macos", "on_linux"):
        lines = [f"  {os_block} do"]
        for os_name, cpu_block, target in PLATFORMS:
            if os_name != os_block:
                continue
            archive = f"felixctl-{tag}-{target}.tar.gz"
            lines += [
                f"    {cpu_block} do",
                f'      url "{base}/{archive}"',
                f'      sha256 "{checksum(dist, archive)}"',
                "    end",
            ]
        lines.append("  end")
        blocks.append("\n".join(lines) + "\n")
    return TEMPLATE.format(version=tag.removeprefix("v"), platforms="\n".join(blocks))


def main() -> int:
    if len(sys.argv) not in (3, 4):
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    tag, dist = sys.argv[1], Path(sys.argv[2])
    repo = sys.argv[3] if len(sys.argv) == 4 else "GetFelix/felix"
    sys.stdout.write(formula(tag, dist, repo))
    return 0


if __name__ == "__main__":
    sys.exit(main())
