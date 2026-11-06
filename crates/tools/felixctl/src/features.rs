//! `features ls` and `features finalize`: the fleet-wide feature switches.

use serde_json::Value;

use crate::cli::{Confirm, FeaturesCommand};
use crate::context::Settings;
use crate::controlplane::{Api, segment};
use crate::manage::ask;
use crate::output::{Output, cell};

pub(crate) async fn run(
    command: &FeaturesCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let api = Api::new(settings)?;
    match command {
        FeaturesCommand::Ls => {
            let features = api.get("/v1/fleet/features", &[]).await?;
            show(out, &features, render_features)
        }
        FeaturesCommand::Finalize {
            feature,
            dry_run,
            confirm,
        } => finalize(&api, feature, *dry_run, *confirm, out).await,
    }
}

async fn finalize(
    api: &Api,
    feature: &str,
    dry_run: bool,
    confirm: Confirm,
    out: &Output,
) -> anyhow::Result<()> {
    if !dry_run {
        ask(
            &format!(
                "Finalize {feature}? This is one-way: brokers without it are refused from \
                 then on."
            ),
            confirm,
        )?;
    }
    let path = format!("/v1/fleet/features/{}/finalize", segment(feature));
    let query = [("dry_run", if dry_run { "true" } else { "false" })];
    let answer = api.post(&path, &query, None).await?;
    show(out, &answer.body.unwrap_or(Value::Null), render_finalize)
}

fn show(out: &Output, response: &Value, render: fn(&Value) -> String) -> anyhow::Result<()> {
    if out.json {
        out.json_value(response)
    } else {
        out.text(&render(response))
    }
}

/// Names comma-joined, or `-` for none.
fn names(value: &Value) -> String {
    let names = cell(value);
    if names.is_empty() {
        "-".to_string()
    } else {
        names.replace(',', ", ")
    }
}

pub(crate) fn render_features(response: &Value) -> String {
    format!(
        "serving brokers: {}\nsupported: {}\nenabled:   {}",
        cell(&response["serving_nodes"]),
        names(&response["supported"]),
        names(&response["enabled"]),
    )
}

pub(crate) fn render_finalize(response: &Value) -> String {
    let feature = cell(&response["feature"]);
    if response["dry_run"] != true {
        return format!("{feature} enabled; brokers without it will be refused from now on");
    }
    if response["enabled"] == true {
        return format!("dry run: {feature} is already enabled");
    }
    if response["would_enable"] == true {
        return format!(
            "dry run: {feature} can be finalized; all {} serving brokers support it",
            cell(&response["serving_nodes"]),
        );
    }
    let requires = names(&response["requires"]);
    if requires != "-" {
        return format!("dry run: {feature} cannot be finalized: finalize {requires} first");
    }
    let lacking = names(&response["lacking"]);
    if lacking == "-" {
        format!("dry run: {feature} cannot be finalized: no broker is serving")
    } else {
        format!("dry run: {feature} cannot be finalized: {lacking} do not support it")
    }
}

#[cfg(test)]
mod tests;
