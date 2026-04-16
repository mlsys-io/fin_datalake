from __future__ import annotations

import json

import typer

import etl.ops.status as status_ops


app = typer.Typer(help="Inspect runtime status.")


@app.command("actors")
def actors_command(
    namespace: str = typer.Option(status_ops.DEFAULT_NAMESPACE, "--namespace", help="Ray namespace to inspect."),
    address: str = typer.Option("auto", "--address", help="Ray address to inspect."),
):
    result = status_ops.list_ray_actors(namespace=namespace, address=address)
    typer.echo(json.dumps(result, indent=2))

