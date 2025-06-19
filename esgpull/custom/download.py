# general
import time
from pathlib import Path

# spatial
import xarray as xa
import dask

# parallel
import concurrent.futures

# rich
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# custom
from esgpull.custom import ui


class DownloadSubset:
    def __init__(self, files, fs, output_dir=None, subset=None, max_workers=4):
        self.files = files
        self.fs = fs
        self.output_dir = output_dir
        self.subset = subset
        self.max_workers = max_workers

    def get_file_path(self, file):
        if self.output_dir:
            return Path(self.output_dir) / file.filename
        else:
            return self.fs[file].drs

    def file_exists(self, file):
        file_path = self.get_file_path(file)
        return file_path.exists()

    def download_and_save_file(self, file, ui: ui.DownloadProgressUI):
        file_path = self.get_file_path(file)
        fname = file.filename
        tmp_file_path = file_path.with_suffix(file_path.suffix + ".part")
        if file_path.exists():
            # Hide this file's progress bar to make room for others
            task_id = ui.file_task_ids[file.file_id]
            ui.set_status(file, "SKIP", "yellow")
            ui.complete_file(file)
            time.sleep(2)
            ui.progress.update(task_id, visible=False)
            return str(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            ui.set_status(file, "DL", "cyan")
            # Set up per-file progress bar for chunk loading
            task_id = ui.file_task_ids[file.file_id]
            ds = xa.open_dataset(
                file.url, engine="h5netcdf", chunks={"time": 2}
            )
            if self.subset:
                missing_dims = []
                for dim in self.subset:
                    if dim not in ds.dims and dim not in ds.coords:
                        missing_dims.append(dim)
                remaining_subset_dims = {
                    k: v
                    for k, v in self.subset.items()
                    if k not in missing_dims
                }
                if remaining_subset_dims:
                    ds = ds.isel(**remaining_subset_dims)
                else:
                    ui.set_status(file, "DL", "yellow")
            # Fine-grained progress: count dask chunks and update rich progress bar
            from dask.callbacks import Callback
            import threading

            class FileChunkProgress(Callback):
                def __init__(self, file, ui, total_chunks, data_var_names):
                    self.file = file
                    self.ui = ui
                    self.total_chunks = total_chunks
                    self.loaded_chunks = 0
                    self.lock = threading.Lock()
                    self.data_var_names = data_var_names

                def _start_state(self, dsk, state):
                    self.ui.update_file_progress(
                        self.file, 0, self.total_chunks
                    )

                def _posttask(self, key, result, dsk, state, id):
                    if (
                        isinstance(key, tuple)
                        and key[0] in self.data_var_names
                    ):
                        with self.lock:
                            self.loaded_chunks += 1
                            self.ui.update_file_progress(
                                self.file, self.loaded_chunks
                            )

            # Accurately count total dask chunks for progress bar
            total_chunks = sum(
                v.data.npartitions
                for v in ds.data_vars.values()
                if hasattr(v.data, "npartitions")
            )
            if total_chunks == 0:
                total_chunks = 1
            data_var_names = {
                v.data.name
                for v in ds.data_vars.values()
                if hasattr(v.data, "name")
            }
            with dask.config.set(scheduler="threads"):
                with FileChunkProgress(file, ui, total_chunks, data_var_names):
                    ds.load()
            ds.to_netcdf(tmp_file_path)
            tmp_file_path.rename(file_path)  # Atomic move to final .nc name
            ui.set_status(file, "DONE", "green")
            ui.complete_file(file)
            return str(file_path)
        except Exception as e:
            ui.set_status(file, "FAIL", "red")
            ui.add_failed(file, f"FAIL {fname}: {e}")
            # print(file.url)
            ui.complete_file(file)
            # Clean up partial file if exists
            if tmp_file_path.exists():
                tmp_file_path.unlink()
            return None

    def download_all_parallel(self):
        """
        Maximize parallelization: open all datasets in parallel (metadata), then trigger parallel Dask loads, then save.
        Integrates with rich progress UI for per-file status and chunk progress.
        """
        import xarray as xr
        from concurrent.futures import ThreadPoolExecutor
        import dask

        files = self.files

        # Step 1: Open all datasets in parallel (fetch metadata)
        def open_ds(file):
            try:
                ds = xr.open_dataset(
                    file.url, engine="h5netcdf", chunks={"time": 2}
                )
                return (file, ds, None)
            except Exception as e:
                return (file, None, e)

        with ui.DownloadProgressUI(files) as ui_instance:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = list(executor.map(open_ds, files))
            datasets = []
            for file, ds, err in results:
                if err is not None or ds is None:
                    ui_instance.set_status(file, "FAIL", "red")
                    ui_instance.add_failed(
                        file, f"FAIL {file.filename}: {err}"
                    )
                    ui_instance.complete_file(file)
                else:
                    ui_instance.set_status(file, "DL", "cyan")
                    datasets.append((file, ds))
            # Step 2: (Optional) Subset
            for i, (file, ds) in enumerate(datasets):
                if self.subset:
                    missing_dims = [
                        dim
                        for dim in self.subset
                        if dim not in ds.dims and dim not in ds.coords
                    ]
                    remaining_subset_dims = {
                        k: v
                        for k, v in self.subset.items()
                        if k not in missing_dims
                    }
                    if remaining_subset_dims:
                        ds = ds.isel(**remaining_subset_dims)
                        datasets[i] = (file, ds)
            # Step 3: Trigger parallel loading of all datasets
            # Setup per-file chunk progress
            from dask.callbacks import Callback
            import threading

            class FileChunkProgress(Callback):
                def __init__(self, file, ui, total_chunks, data_var_names):
                    self.file = file
                    self.ui = ui
                    self.total_chunks = total_chunks
                    self.loaded_chunks = 0
                    self.lock = threading.Lock()
                    self.data_var_names = data_var_names

                def _start_state(self, dsk, state):
                    self.ui.update_file_progress(
                        self.file, 0, self.total_chunks
                    )

                def _posttask(self, key, result, dsk, state, id):
                    if (
                        isinstance(key, tuple)
                        and key[0] in self.data_var_names
                    ):
                        with self.lock:
                            self.loaded_chunks += 1
                            self.ui.update_file_progress(
                                self.file, self.loaded_chunks
                            )

            # Build all chunk progress callbacks
            callbacks = []
            load_calls = []
            for file, ds in datasets:
                # Accurately count total dask chunks for progress bar
                total_chunks = sum(
                    v.data.npartitions
                    for v in ds.data_vars.values()
                    if hasattr(v.data, "npartitions")
                )
                if total_chunks == 0:
                    total_chunks = 1
                data_var_names = {
                    v.data.name
                    for v in ds.data_vars.values()
                    if hasattr(v.data, "name")
                }
                callbacks.append(
                    FileChunkProgress(file, ui, total_chunks, data_var_names)
                )
                load_calls.append(ds.load)
            # Run all loads in parallel with Dask
            try:
                with dask.config.set(scheduler="threads"):
                    with dask.callbacks.CallbackChain(*callbacks):
                        dask.compute(*[ds.load() for _, ds in datasets])
                # Step 4: Save all datasets
                for file, ds in datasets:
                    file_path = self.get_file_path(file)
                    ds.to_netcdf(file_path)
                    ui.set_status(file, "DONE", "green")
                    ui.complete_file(file)
            except Exception as e:
                for file, ds in datasets:
                    ui.set_status(file, "FAIL", "red")
                    ui.add_failed(file, f"FAIL {file.filename}: {e}")
                    ui.complete_file(file)
            ui.print_summary()

    def completion_message(self):
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
        console.print(
            Panel(table, title="[cyan]Download Complete", border_style="cyan")
        )

    def run(self):
        file_str = "files" if len(self.files) > 1 else "file"
        print(f"Attempting download of {len(self.files)} {file_str}...\n")
        # Use new parallel method for >1 file
        # if len(self.files) > 1:
        #     self.download_all_parallel()
        # else:
        with ui.DownloadProgressUI(self.files) as ui_instance:

            def wrapped_download(file):
                return self.download_and_save_file(file, ui_instance)

            try:
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.max_workers
                ) as executor:
                    list(executor.map(wrapped_download, self.files))
            except KeyboardInterrupt:
                print(
                    "\nDownload interrupted by user. Shutting down workers..."
                )
                executor.shutdown(wait=False, cancel_futures=True)
            finally:
                ui_instance.print_summary()
