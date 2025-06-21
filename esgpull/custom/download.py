# general
import time
from pathlib import Path
import logging
import contextlib
import threading

# third-party
from aiohttp import ClientResponseError


# spatial
import xarray as xa
import dask

# parallel
import concurrent.futures
from concurrent.futures import TimeoutError

# rich
from rich.console import Console

# from rich.table import Table
# from rich.panel import Panel

# custom
from esgpull.custom import ui


class DownloadSubset:
    def __init__(
        self,
        files,
        fs,
        output_dir=None,
        subset=None,
        max_workers=4,
        batch_size=50,
    ):
        self.files = files
        self.fs = fs
        self.output_dir = output_dir
        self.subset = subset
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.last_activity_time = 0

    def _reset_activity_timer(self):
        self.last_activity_time = time.time()

    def get_file_path(self, file):
        if self.output_dir:
            return Path(self.output_dir) / file.filename
        else:
            return self.fs[file].drs

    def file_exists(self, file):
        file_path = self.get_file_path(file)
        return file_path.exists()

    def _save_file(self, ui, ds, file):
        file_path = self.get_file_path(file)
        tmp_file_path = file_path.with_suffix(file_path.suffix + ".part")
        try:
            ui.set_status(file, "SAVING", "yellow")
            tmp_file_path.parent.mkdir(parents=True, exist_ok=True)
            ds.to_netcdf(tmp_file_path)
            tmp_file_path.rename(file_path)
            ui.set_status(file, "DONE", "green")
        except Exception as e:
            ui.set_status(file, "FAIL", "red")
            ui.add_failed(file, f"FAIL (save): {e}", exc_info=e)
        finally:
            ui.complete_file(file)

    def run(self):
        file_str = "files" if len(self.files) > 1 else "file"
        console = Console()
        console.print(
            f"Attempting download of {len(self.files)} {file_str} in batches of {self.batch_size}..."
        )
        start_time = time.localtime()
        console.print(
            f":clock3: START: {time.strftime('%Y-%m-%d %H:%M:%S', start_time)}\n"
        )

        # Helper class for Dask progress, defined once
        from dask.callbacks import Callback

        class FileChunkProgress(Callback):
            def __init__(
                self, downloader, file, ui, total_chunks, data_var_names
            ):
                self.downloader = downloader
                self.file = file
                self.ui = ui
                self.total_chunks = total_chunks
                self.loaded_chunks = 0
                self.lock = threading.Lock()
                self.data_var_names = data_var_names

            def _start_state(self, dsk, state):
                self.ui.update_file_progress(self.file, 0, self.total_chunks)
                self.downloader._reset_activity_timer()

            def _posttask(self, key, result, dsk, state, id):
                if isinstance(key, tuple) and key[0] in self.data_var_names:
                    with self.lock:
                        self.downloader._reset_activity_timer()
                        self.loaded_chunks += 1
                        self.ui.update_file_progress(
                            self.file, self.loaded_chunks
                        )

        # Helper function to open a single dataset lazily, handling URL fallback
        def open_ds(file):
            try:
                # First attempt with original URL
                ds = xa.open_dataset(
                    file.url, engine="h5netcdf", chunks={"time": 2}
                )
                return (file, ds, None)
            except (ClientResponseError, FileNotFoundError):
                # Fallback with #mode=bytes
                try:
                    url_bytes_mode = f"{file.url}#mode=bytes"
                    ds = xa.open_dataset(
                        url_bytes_mode, engine="h5netcdf", chunks={"time": 2}
                    )
                    return (file, ds, None)
                except Exception as e:
                    return (file, None, e)  # Fallback failed
            except Exception as e:
                return (
                    file,
                    None,
                    e,
                )  # Initial attempt failed for other reason

        with ui.DownloadProgressUI(self.files) as ui_instance:
            threads = []

            def complete_file_after_delay(ui, file, delay):
                time.sleep(delay)
                task_id = ui.file_task_ids.get(file.file_id)
                if task_id is not None:
                    ui.progress.update(task_id, visible=False)
                    ui.progress.advance(ui.overall_task)

            try:
                num_batches = (
                    len(self.files) + self.batch_size - 1
                ) // self.batch_size
                for i in range(0, len(self.files), self.batch_size):
                    initial_batch_files = self.files[i : i + self.batch_size]
                    batch_files = []
                    for file in initial_batch_files:
                        if self.file_exists(file):
                            ui_instance.set_status(file, "SKIPPED", "yellow")
                            thread = threading.Thread(
                                target=complete_file_after_delay,
                                args=(ui_instance, file, 2),
                            )
                            thread.start()
                            threads.append(thread)
                        else:
                            batch_files.append(file)

                    if not batch_files:
                        continue

                    console.print(
                        f"\n[bold cyan]Processing batch {i // self.batch_size + 1}/{num_batches}...[/bold cyan]"
                    )

                    # Step 1: Open all datasets in the batch in parallel (metadata only)
                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=self.max_workers
                    ) as executor:
                        results = list(executor.map(open_ds, batch_files))

                    datasets_to_process = []
                    for file, ds, err in results:
                        if err is not None or ds is None:
                            ui_instance.set_status(file, "FAIL", "red")
                            ui_instance.add_failed(
                                file, f"FAIL (open): {err}", exc_info=err
                            )
                            ui_instance.complete_file(file)
                        else:
                            # Apply subsetting if needed
                            if self.subset:
                                missing_dims = [
                                    dim
                                    for dim in self.subset
                                    if dim not in ds.dims
                                    and dim not in ds.coords
                                ]
                                remaining_subset_dims = {
                                    k: v
                                    for k, v in self.subset.items()
                                    if k not in missing_dims
                                }
                                if remaining_subset_dims:
                                    ds = ds.isel(**remaining_subset_dims)

                            datasets_to_process.append(
                                {"file": file, "ds": ds}
                            )

                    if not datasets_to_process:
                        continue

                    # Step 2: Let Dask compute all datasets in the batch in parallel
                    callbacks = []
                    computations = []
                    for item in datasets_to_process:
                        file, ds = item["file"], item["ds"]
                        total_chunks = (
                            sum(
                                v.data.npartitions
                                for v in ds.data_vars.values()
                                if hasattr(v.data, "npartitions")
                            )
                            or 1
                        )
                        data_var_names = {
                            v.data.name
                            for v in ds.data_vars.values()
                            if hasattr(v.data, "name")
                        }
                        callbacks.append(
                            FileChunkProgress(
                                self,
                                file,
                                ui_instance,
                                total_chunks,
                                data_var_names,
                            )
                        )
                        computations.append(
                            ds
                        )  # We want to compute the ds object
                        ui_instance.set_status(file, "LOADING", "cyan")

                    try:
                        with dask.config.set(scheduler="threads"):
                            with contextlib.ExitStack() as stack:
                                for cb in callbacks:
                                    stack.enter_context(cb)

                                from concurrent.futures import (
                                    ThreadPoolExecutor,
                                )

                                # Use an executor for the entire dask.compute call to apply a timeout
                                executor = ThreadPoolExecutor(max_workers=1)
                                future = executor.submit(
                                    dask.compute,
                                    *computations,
                                    scheduler="threads",
                                )
                                # The result of dask.compute is a tuple of loaded datasets
                                # loaded_data_tuple = future.result(
                                #     timeout=1200
                                # )  # 20-minute timeout for the whole batch

                                self._reset_activity_timer()
                                inactivity_timeout = 120  # 2 minutes
                                loaded_data_tuple = None
                                while True:
                                    try:
                                        loaded_data_tuple = future.result(
                                            timeout=1
                                        )
                                        break  # Succeeded
                                    except TimeoutError:
                                        if (
                                            time.time()
                                            - self.last_activity_time
                                            > inactivity_timeout
                                        ):
                                            future.cancel()  # Attempt to cancel the underlying task
                                            raise TimeoutError(
                                                f"Inactivity timeout of {inactivity_timeout}s exceeded for batch."
                                            )

                        # Step 3: Save the loaded datasets
                        for i, item in enumerate(datasets_to_process):
                            file = item["file"]
                            # The loaded data is a tuple of xarray.Dataset objects
                            loaded_ds = loaded_data_tuple[i]
                            self._save_file(ui_instance, loaded_ds, file)

                    except TimeoutError:
                        logging.error(
                            f"Dask compute timed out for batch starting with {datasets_to_process[0]['file'].filename}"
                        )
                        for item in datasets_to_process:
                            file = item["file"]
                            ui_instance.set_status(file, "FAIL", "red")
                            ui_instance.add_failed(
                                file, "FAIL: Batch timed out during compute"
                            )
                            ui_instance.complete_file(file)
                        continue  # Move to the next batch
                    except Exception as e:
                        logging.error(
                            f"Dask compute failed for batch: {e}",
                            exc_info=True,
                        )
                        for item in datasets_to_process:
                            file = item["file"]
                            ui_instance.set_status(file, "FAIL", "red")
                            ui_instance.add_failed(
                                file, f"FAIL (compute): {e}", exc_info=e
                            )
                            ui_instance.complete_file(file)

            except KeyboardInterrupt:
                console.print(
                    "\n[bold red]Download interrupted by user. Shutting down workers...[/bold red]"
                )
            finally:
                for thread in threads:
                    thread.join()
                ui_instance.print_summary()

        end_time = time.localtime()
        console.print(
            f"\n:clock3: END: {time.strftime('%Y-%m-%d %H:%M:%S', end_time)}"
        )
        elapsed = time.mktime(end_time) - time.mktime(start_time)
        console.print(
            f":white_check_mark: Script completed in {elapsed:.1f} seconds."
        )
