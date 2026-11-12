#!/usr/bin/env bash
# Shared plumbing for the comparison session, same idea as the Felix harness's
# lib.sh: the operator never SSHes in. Every operator->VM step goes through
# `az vm run-command invoke` over HTTPS. Scripts shipped to run-command run under
# dash as root, so remote snippets must be POSIX sh and end by printing __RUNOK__
# (run-command reports delivery success even when the script itself failed).

run_on() {
  _vm="$1"; _script="$2"
  _msg="$(az vm run-command invoke \
    --resource-group "${GROUP}" --name "${_vm}" \
    --command-id RunShellScript --scripts @"${_script}" \
    --query 'value[0].message' -o tsv 2>&1)" || {
    printf '%s\n' "${_msg}" >&2
    return 1
  }
  printf '%s\n' "${_msg}"
  printf '%s' "${_msg}" | grep -q '__RUNOK__' || {
    echo "!! remote script on ${_vm} did not complete (no __RUNOK__)" >&2
    return 1
  }
}

run_on_str() {
  _vm="$1"; _text="$2"
  _tmp="$(mktemp "${TMPDIR:-/tmp}/cmp-run.XXXXXX")"
  chmod 600 "${_tmp}"
  printf '%s' "${_text}" > "${_tmp}"
  run_on "${_vm}" "${_tmp}"
  _rc=$?
  rm -f "${_tmp}"
  return ${_rc}
}

extract_between() {
  _tag="$1"
  sed -n "s/.*__${_tag}_BEGIN__\(.*\)__${_tag}_END__.*/\1/p" | head -n1
}

# Deterministic VM names — compare.bicep's prefix is 'cmp'.
broker_vm() { echo "cmp-broker-$1"; }
client_vm() { echo "cmp-client-$1"; }
