from collections import Counter
from collections.abc import Sequence
import json

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import init_esgpull
from esgpull.models import FileStatus, sql
from esgpull.tui import Verbosity


@click.command()
@args.status
@opts.verbosity
@click.option('--subset-criteria', type=str, help='Subset criteria for filtering files (JSON format).')
def retry(
    status: Sequence[FileStatus],
    verbosity: Verbosity,
    subset_criteria: str | None,
):
    """
    Re-queue failed and cancelled downloads
    """
    subset_criteria_dict = json.loads(subset_criteria) if subset_criteria else None
    if not status:
        status = FileStatus.retryable()
    esg = init_esgpull(verbosity)
    with esg.ui.logging("retry", onraise=Abort):
        assert FileStatus.Done not in status
        assert FileStatus.Queued not in status
        files = list(esg.db.scalars(sql.file.with_status(*status)))
        status_str = "/".join(f"[bold red]{s.value}[/]" for s in status)
        if not files:
            esg.ui.print(f"No {status_str} files found.")
            raise Exit(0)
        counts = Counter(file.status for file in files)
        filtered_files = []
        for file in files:
            if subset_criteria_dict and not all(
                getattr(file, key, None) == value for key, value in subset_criteria_dict.items()
            ):
                continue
            file.status = FileStatus.Queued
            filtered_files.append(file)
        esg.db.add(*filtered_files)
        msg = "Sent back to the queue: "
        msg += ", ".join(
            f"{count} [bold red]{status.value}[/]"
            for status, count in counts.items()
        )
        esg.ui.print(msg)
