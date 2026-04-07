"""
CLI for managing Ray Serve agent deployments.
"""

from __future__ import annotations

import json

import typer

from etl.agents.manager import (
    baseline_fleet_specs,
    delete_agent,
    delete_baseline_fleet,
    deploy_agent,
    deploy_baseline_fleet,
    list_fleet_state,
)


app = typer.Typer(help="Manage Ray Serve agent deployments and fleet state.")


def _parse_json_option(raw: str, *, label: str) -> dict:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"{label} must be valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise typer.BadParameter(f"{label} must decode to a JSON object.")
    return value


@app.command("deploy")
def deploy_command(
    agent_class: str,
    name: str = typer.Option(..., "--name", help="Deployment/app name."),
    num_replicas: int = typer.Option(1, help="Serve replica count."),
    num_cpus: float = typer.Option(0.5, help="CPU reservation per replica."),
    config: str = typer.Option("{}", help="JSON object passed as agent config."),
    serve_options: str = typer.Option("{}", help="JSON object of Serve deployment options."),
):
    deploy_agent(
        agent_class,
        name=name,
        num_replicas=num_replicas,
        num_cpus=num_cpus,
        config=_parse_json_option(config, label="config"),
        serve_options=_parse_json_option(serve_options, label="serve_options"),
    )
    typer.echo(f"Deployed {agent_class} as {name}")


@app.command("delete")
def delete_command(
    name: str,
    clean_catalog: bool = typer.Option(
        True,
        "--clean-catalog/--keep-catalog",
        help="Delete the durable catalog entry too. Clean catalog is recommended for intentional removal.",
    ),
):
    result = delete_agent(name, clean_catalog=clean_catalog)
    typer.echo(json.dumps({"name": name, **result}, indent=2))


@app.command("deploy-baseline")
def deploy_baseline_command():
    handles = deploy_baseline_fleet()
    typer.echo(f"Deployed baseline fleet: {', '.join(sorted(handles))}")


@app.command("delete-baseline")
def delete_baseline_command(
    clean_catalog: bool = typer.Option(
        True,
        "--clean-catalog/--keep-catalog",
        help="Delete baseline catalog entries too.",
    ),
):
    result = delete_baseline_fleet(clean_catalog=clean_catalog)
    typer.echo(json.dumps(result, indent=2))


@app.command("list")
def list_command():
    state = list_fleet_state()
    typer.echo(json.dumps(state, indent=2))


@app.command("profiles")
def profiles_command():
    typer.echo(json.dumps({"baseline": [spec.__dict__.copy() for spec in baseline_fleet_specs()]}, indent=2))


def main() -> None:
    app()


if __name__ == "__main__":
    main()
