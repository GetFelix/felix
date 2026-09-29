# Self-hosted CI runners in Azure

Two persistent Ubuntu 24.04 VMs (`Standard_D8as_v5`, 8 vCPU, 256 GB Premium SSD)
in resource group `felix-ci-runners`, region `eastus`, registered to
`gabloe/felix` with the label `felix-azure`. They take the heavy jobs: CI's
`test`, `coverage`, the nightly history campaign and the nightly fuzz matrix.
Which jobs and why is in `docs-site/src/content/docs/development/building.md`.

```bash
./deploy.sh up         # group, NSG, VNet, VMs; waits for cloud-init (~15 min)
./deploy.sh register   # register or re-register every VM
./deploy.sh status     # what GitHub sees
./deploy.sh down       # deregister and delete everything
```

`GROUP`, `LOCATION`, `SIZE` and `COUNT` override the defaults. `eastus` because
the Azure perf sessions use westus3, centralus and eastus2, and the
subscription caps Dasv5 at 20 vCPUs per region: two 8-vCPU runners there would
leave the perf cells no room.

## Security model

- **Nothing inbound.** The subnet NSG denies all inbound traffic at priority
  100, ahead of Azure's defaults. The public IP exists only because new subnets
  have no default outbound path. Administration goes through
  `az vm run-command invoke`, which rides the Azure control plane. The SSH key
  in `~/.ssh/felix-ci-runners` is there because Azure requires one; nothing
  can reach port 22.
- **No standing credentials on the VMs.** No managed identity and no PAT.
  `register` fetches a one-hour registration token per VM with
  `gh api -X POST repos/gabloe/felix/actions/runners/registration-token` and
  hands it over run-command. The runner then holds only its own runner
  credential.
- **Trusted events only.** The repo is public, and a persistent runner that ran
  a fork's PR would run that fork's code with the next job's secrets in reach.
  Every job routed here uses:

  ```yaml
  runs-on: ${{ (github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository) && fromJSON('["self-hosted","felix-azure"]') || 'ubuntu-latest' }}
  ```

  so a fork PR falls back to GitHub-hosted. Fork PRs also need approval from a
  maintainer before any workflow runs (repo setting
  `approval_policy=all_external_contributors`).
- The `runner` user has no sudo, but it is in the `docker` group because the
  test and coverage jobs use a Postgres service container. That is root on the
  host in practice, which is one more reason for the rule above.

## What is installed

`cloud-init.yaml`: build-essential, clang, lld, cmake, libssl-dev, Java 21,
Docker, Node 20, Python 3 with PyYAML, `gh`, `postgresql-client`, rustup with
1.97.1 (clippy, rustfmt, llvm-tools) and nightly, `cargo-llvm-cov`, and the
Actions runner under `/home/runner/actions-runner`. The runner updates itself.
Bump `RUST_TOOLCHAIN` there when `rust-toolchain.toml` moves; the workflows'
`dtolnay/rust-toolchain` step installs a missing toolchain anyway, so a stale
image only costs one download.

## Target directories

Hosted jobs use `swatinem/rust-cache`. On these machines that would upload and
download gigabytes per run for nothing, so the self-hosted path skips it and
runs `.github/actions/warm-target` instead:

- Each job gets its own slot, `~/felix-cache/<job>` (`ci-test`, `coverage`,
  `history`, `fuzz-nightly`). Slots are per job because the jobs build with
  different flags (llvm-cov instrumentation, nightly sanitizers) and would
  evict each other.
- The checkout's `target` becomes a symlink to the slot and `CARGO_TARGET_DIR`
  points at it. Both are needed: the Taskfile names `target/...` by relative
  path, and `actions/checkout` wipes untracked files, so a real `target/`
  directory would not survive.
- Cargo never garbage-collects, so a slot over 100 GB is wiped at the start of
  the next run. The coverage job also runs `cargo llvm-cov clean --workspace`
  first, because `task coverage` uses `--no-clean` and would otherwise merge
  the previous run's profiles.
- Docker images older than a week are pruned by a weekly cron job.

To start a slot cold, delete it via run-command:

```bash
az vm run-command invoke -g felix-ci-runners -n felix-ci-runner-1 \
  --command-id RunShellScript --scripts 'rm -rf /home/runner/felix-cache/ci-test'
```

## Capacity

Two runners, one job each. When both are busy, jobs queue. The nightly fuzz
matrix (twelve targets) and the history campaign are scheduled for the same
time and hold both runners for a few hours; a push in that window waits.
