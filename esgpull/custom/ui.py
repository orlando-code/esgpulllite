# rich
from rich.progress import Progress, BarColumn


class DownloadProgressUI:
    """Manages rich progress bars for overall and per-file download, with status in the bar description."""

    def __init__(self, files):
        self.files = files
        self.progress = Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            "[progress.completed]{task.completed}/{task.total}",
            transient=True,
            expand=True,
            auto_refresh=True,
            refresh_per_second=10,
        )
        self.overall_task = None
        self.status_counts = {"done": 0, "skipped": 0, "failed": 0}
        self.failed_files = []  # List of (file, error_message)
        self.file_task_ids = {}  # file -> task_id
        self.file_status = {}  # file -> status string

    def __enter__(self):
        self.progress.__enter__()
        self.overall_task = self.progress.add_task(
            "[cyan]Overall\n", total=len(self.files)
        )
        # Add a progress bar for each file, with initial status 'PENDING'
        for file in self.files:
            fname = file.filename
            desc = f"[white][PENDING] {fname}"
            task_id = self.progress.add_task(desc, total=1, visible=True)
            self.file_task_ids[file.file_id] = task_id
            self.file_status[file.file_id] = "PENDING"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.__exit__(exc_type, exc_val, exc_tb)

    def set_status(self, file, status, color=None):
        """Update the status for a file and update the progress bar description."""
        fname = file.filename
        color = color or "white"
        desc = f"[{color}][{status}] {fname}"
        task_id = self.file_task_ids[file.file_id]
        self.progress.update(task_id, description=desc)
        self.file_status[fname] = status
        if status == "DONE":
            self.status_counts["done"] += 1
        elif status == "SKIP":
            self.status_counts["skipped"] += 1
        elif status == "FAIL":
            self.status_counts["failed"] += 1

    def add_failed(self, file, msg):
        self.failed_files.append((file, msg))

    def complete_file(self, file):
        # Advance overall progress and mark file bar as complete
        task_id = self.file_task_ids[file.file_id]
        self.progress.update(
            task_id, completed=self.progress.tasks[task_id].total
        )
        self.progress.advance(self.overall_task)

    def update_file_progress(self, file, completed, total=None):
        task_id = self.file_task_ids[file.file_id]
        if total is not None:
            self.progress.update(task_id, completed=completed, total=total)
        else:
            self.progress.update(task_id, completed=completed)

    def print_summary(self):
        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel

        console = Console()
        table = Table(
            title="Download Summary",
            show_header=True,
            header_style="bold magenta",
        )
        table.add_column("Status", style="bold")
        table.add_column("Count", style="bold")
        table.add_row(
            "[green]Completed[/green]", str(self.status_counts["done"])
        )
        table.add_row(
            "[yellow]Skipped[/yellow]", str(self.status_counts["skipped"])
        )
        table.add_row("[red]Failed[/red]", str(self.status_counts["failed"]))
        console.print(table)
        if self.failed_files:
            file, msg = self.failed_files[0]
            console.print(
                Panel(
                    f"Example failed file: [bold]{file.filename}[/bold]\n[red]{msg}[/red]",
                    title="[red]Failure Example[/red]",
                    style="red",
                )
            )
