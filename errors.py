import typer
from typing import Any


def panic_if_empty(value, name, interactive=None):
    if value is None and interactive is not None and not interactive:
        panic(
            f"Error: No {name} has been supplied and application is not in interactive mode."
        )
    elif value is None and interactive is None:
        panic(f"Error: No {name} has been supplied.")
    elif value is None and interactive:
        panic(f"Error: No {name} has been supplied. Application is interactive.")
    return None


def panic_empty_value(value):
    panic(
        f"Error: No {value} has been supplied and application not in interactive mode."
    )


def panic(message, logger: Any = None):
    msg = f"Error: {message}"
    if not logger:
        typer.echo(msg)
    else:
        typer.echo(msg, logger)
    raise typer.Exit(code=1)
