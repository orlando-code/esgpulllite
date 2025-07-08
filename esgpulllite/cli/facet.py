import click
from click.exceptions import Abort

from esgpulllite.cli.decorators import args, opts
from esgpulllite.cli.utils import init_esgpull
from esgpulllite.models import sql
from esgpulllite.tui import Verbosity


@click.command()
@args.key
@opts.verbosity
def facet(
    key: str | None,
    verbosity: Verbosity,
):
    esg = init_esgpull(verbosity)
    with esg.ui.logging("facet", onraise=Abort):
        if key is None:
            results = esg.db.scalars(sql.facet.names())
        else:
            results = esg.db.scalars(sql.facet.values(key))
        esg.ui.print(sorted(results))
