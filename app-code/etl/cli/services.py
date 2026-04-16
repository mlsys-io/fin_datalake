from __future__ import annotations

import json

import typer

import etl.ops.service_ops as service_ops


app = typer.Typer(help="Manage named Ray services and actors.")


@app.command("delete")
def delete_service_command(
    name: str = typer.Argument(..., help="Named Ray service or actor to delete."),
    address: str = typer.Option("", "--address", help="Ray cluster address override. Falls back to RAY_ADDRESS when omitted."),
    context_key: list[str] = typer.Option(
        [],
        "--context-key",
        help="Optional context key to clear after deleting the service. Repeat to clear multiple keys.",
    ),
):
    result = service_ops.delete_named_service(name, context_keys=context_key, address=address or None)
    typer.echo(json.dumps(result, indent=2))

