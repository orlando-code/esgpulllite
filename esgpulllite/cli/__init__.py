#!/usr/bin/env python3

# https://click.palletsprojects.com/en/latest/
import click

from esgpulllite import __version__
from esgpulllite.cli.add import add
from esgpulllite.cli.config import config
from esgpulllite.cli.convert import convert
from esgpulllite.cli.datasets import datasets
from esgpulllite.cli.download import download
from esgpulllite.cli.login import login
from esgpulllite.cli.remove import remove
from esgpulllite.cli.retry import retry
from esgpulllite.cli.search import search
from esgpulllite.cli.self import self
from esgpulllite.cli.show import show
from esgpulllite.cli.status import status
from esgpulllite.cli.track import track, untrack
from esgpulllite.cli.update import update
from esgpulllite.tui import UI

# from esgpulllite.cli.autoremove import autoremove
# from esgpulllite.cli.facet import facet
# from esgpulllite.cli.get import get
# from esgpulllite.cli.install import install

# [-]TODO: stats
#   - speed per index/data node
#   - total disk usage
#   - log config for later optimisation ?

SUBCOMMANDS: list[click.Command] = [
    add,
    # autoremove,
    config,
    convert,
    datasets,
    download,
    # facet,
    # get,
    self,
    # install,
    login,
    remove,
    retry,
    search,
    show,
    track,
    untrack,
    status,
    # # stats,
    update,
]

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

_ui = UI("/tmp")
version_msg = _ui.render(f"esgpull, version [green]{__version__}[/]")


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(None, "-V", "--version", message=version_msg)
def cli():
    """
    esgpull is a management utility for files and datasets from ESGF.
    """


for subcmd in SUBCOMMANDS:
    cli.add_command(subcmd)


def main():
    cli()
