# Self-hosted CI runners in Azure

Four persistent Ubuntu 24.04 VMs (`Standard_D8as_v5`, 8 vCPU, 256 GB Premium SSD)
registered to `GetFelix/felix` with the label `felix-azure`:

| VMs | Resource group | Region |
|---|---|---|
| `felix-ci-runner-1`, `-2` | `felix-ci-runners` | `eastus` |
| `felix-ci-runner-3`, `-4` | `felix-ci-runners-westus2` | `westus2` |

They take the heavy jobs (CI's `test`, `coverage`, the nightly history
campaign and the nightly power-loss sweep) for pushes to `main`, schedules and
manual runs. Pull requests run those jobs on GitHub-hosted runners. The
nightly fuzz matrix stays on GitHub-hosted runners too: each target fuzzes on
one core, so eight cores buy it little.
Which jobs and why is in `docs-site/src/content/docs/development/building.md`.

```bash
./deploy.sh up         # group, NSG, VNet, VMs; waits for cloud-init (~15 min)
./deploy.sh register   # register or re-register every VM
./deploy.sh status     # what GitHub sees
./deploy.sh down       # deregister and delete everything
```

`GROUP`, `LOCATION`, `SIZE`, `COUNT` and `FIRST` (the first VM's number)
override the defaults, which describe the eastus pair. Each region is its own
resource group, so pass the same variables to every command for the westus2
pair:

```bash
GROUP=felix-ci-runners-westus2 LOCATION=westus2 FIRST=3 COUNT=2 ./deploy.sh up
GROUP=felix-ci-runners-westus2 FIRST=3 COUNT=2 ./deploy.sh register
```

Two regions because the subscription caps vCPUs at 20 per region, which fits
two 8-vCPU runners, and the Azure perf sessions use westus3, centralus and
eastus2: runners there would leave the perf cells no room.

## Security model

- **Nothing inbound.** The subnet NSG denies all inbound traffic at priority
  100, ahead of Azure's defaults. The public IP exists only because new subnets
  have no default outbound path. Administration goes through
  `az vm run-command invoke`, which rides the Azure control plane. The SSH key
  in `~/.ssh/felix-ci-runners` is there because Azure requires one; nothing
  can reach port 22.
- **No standing credentials on the VMs.** No managed identity and no PAT.
  `register` fetches a one-hour registration token per VM with
  `gh api -X POST repos/GetFelix/felix/actions/runners/registration-token` and
  hands it over run-command. The runner then holds only its own runner
  credential.
- **No pull requests.** The repo is public, and a persistent runner that ran
  a fork's PR would run that fork's code with the next job's secrets in reach.
  Every job routed here uses:

  ```yaml
  runs-on: ${{ github.event_name != 'pull_request' && fromJSON('["self-hosted","felix-azure"]') || 'ubuntu-latest' }}
  ```

  so every PR, fork or not, runs on GitHub-hosted and never waits for these
  machines. Fork PRs also need approval from a
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
  `history`, `power-loss`). Slots are per job because the
  jobs build with different flags (llvm-cov instrumentation, nightly
  sanitizers) and would evict each other.
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

Four runners, one job each. When all four are busy, jobs queue. PRs never
wait for them. The nightly history campaign holds one runner for about half an
hour.
