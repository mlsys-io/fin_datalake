from __future__ import annotations

import typer

from etl.cli import agents, context, services, status


app = typer.Typer(help="Manage ETL agents, runtime services, and shared context state.")

app.add_typer(agents.app, name="agents")
app.add_typer(services.app, name="services")
app.add_typer(context.app, name="context")
app.add_typer(status.app, name="status")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
