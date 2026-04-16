from __future__ import annotations

import json

import typer

import etl.context.store as context_store


app = typer.Typer(help="Inspect and clean shared context-store state.")


@app.command("delete")
def delete_context_command(
    key: str = typer.Argument(..., help="Context store key to delete."),
):
    result = context_store.delete_context_keys([key])
    typer.echo(json.dumps({"key": key, "deleted": result.get(key, False)}, indent=2))

