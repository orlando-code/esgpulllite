# general
import re
import numpy as np
import pandas as pd
import xarray as xa
import time
import hashlib
import time

# fileops
from pathlib import Path
import concurrent.futures
import threading
import asyncio
import shutil
import dask
from concurrent.futures import ThreadPoolExecutor

# spatial
import xesmf as xe

# esgpull
from esgpull.api import EsgpullAPI
from esgpull.models import File

# formatting
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, TaskID, BarColumn
from rich.progress import Progress
from dask.diagnostics import ProgressBar


def read_yaml(file_path):
    import yaml
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


class SearchResults:
    """
    A class to hold search results from the Esgpull API.
    It can be used to filter, sort, and manipulate the results.
    """
    def __init__(self, search_criteria: dict, meta_criteria: dict):
        self.search_criteria = search_criteria
        self.meta_criteria = meta_criteria
        self.top_n = meta_criteria.get('top_n', 3)  # Default to 10 if not specified
        self.search_results = []  # List to hold File objects
        self.results_df = None  # DataFrame to hold results for further processing
        self.results_df_top = None  # DataFrame for top N results
        self.search_results_fp = None
        self.fs = EsgpullAPI().esg.fs  # File system from Esgpull API

    def load_config(self, config_path: str) -> None:
        """Load search criteria and metadata from a YAML configuration file."""
        config = read_yaml(config_path)
        self.search_criteria = config.get('search_criteria', {})
        self.meta_criteria = config.get('meta_criteria', {})
        self.top_n = self.meta_criteria.get('top_n', 3)
        self.search_criteria['limit'] = self.meta_criteria.get('limit', 4)  # Default limit if not specified
        
    def do_search(self) -> None:
        """Perform a search using the provided criteria and populate results."""
        api = EsgpullAPI()
        results = api.search(criteria=self.search_criteria)
        self.results_df = pd.DataFrame(results)
        if '_sa_instance_state' in self.results_df.columns:
            self.results_df = self.results_df.drop(columns=['_sa_instance_state'])
        if not self.results_df.empty:
            self.sort_results_by_metadata()
        else:
            print("[SearchResults] No results found for given criteria.")

    def sort_results_by_metadata(self) -> None:
        """Sort a list of File objects by institution_id, source_id, experiment_id, member_id."""
        if self.results_df is None or self.results_df.empty:
            print("[SearchResults] No results to sort.")
            return
        # convert resolutions to float for sorting
        resolutions = self.results_df.apply(lambda f: self.calc_resolution(f.nominal_resolution), axis=1)
        self.results_df['nominal_resolution'] = resolutions
        self.results_df = self.results_df.sort_values(
            by=["institution_id", "source_id", "experiment_id", "member_id", "nominal_resolution"],
            ascending=[True, True, True, True, True],
            na_position="last"
        ).reset_index(drop=True)
        # Update self.search_results to match the sorted DataFrame
        self.search_results = [File(**{k: v for k, v in row.items() if k != '_sa_instance_state'}) for _, row in self.results_df.iterrows()]

    def calc_resolution(self, res) -> float:
        """
        Extract nominal resolution from file.nominal_resolution and return in degrees.
        Supports 'xx km', 'x x degree', or 'x degree'. Returns large value if unknown.
        Handles both string and numeric input.
        """
        if isinstance(res, (float, int)):
            return float(res)
        if not res:
            return 9999.0
        res = str(res).lower().replace(" ", "")
        if m := re.match(r"([\d.]+)km", res):
            return float(m.group(1)) / 111.0
        if m := re.match(r"([\d.]+)x([\d.]+)degree", res):
            return (float(m.group(1)) + float(m.group(2))) / 2.0
        if m := re.match(r"([\d.]+)degree", res):
            return float(m.group(1))
        return 9999.0
        
    def search_message(self, search_state: str) -> None:
        """Display a nicely formatted search message using rich."""
        console = Console()
        if search_state == 'pre':
            table = Table(title="Search Criteria", show_header=True, header_style="bold magenta")
            table.add_column("Key", style="dim", width=20)
            table.add_column("Value", style="bold")
            for k, v in self.search_criteria.items():
                table.add_row(str(k), str(v))
            console.print(Panel(table, title="[cyan]Starting Search", border_style="cyan"))
        elif search_state == 'post':
            if len(self.search_results) == self.meta_criteria.get('limit', None):
                match_msg = ("[orange1](limit of search search reached)[/orange1]")
            else:
                match_msg = ""
            msg = f"[green]Search completed.[/green] [bold]{len(self.search_results)}[/bold] files {match_msg} found matching criteria."
            console.print(Panel(msg, title="[green]Search Results", border_style="green"))

    def get_top_n(self) -> pd.DataFrame:
        """
        Return all files associated with the top n groups, 
        where groups are defined by ['institution_id', 'source_id', 'experiment_id'].
        """
        if self.results_df is None:
            raise ValueError("No results to select from. Run do_search() first.")
        # Identify the top n groups by group size (descending)
        group_keys = ['institution_id', 'source_id', 'experiment_id']
        group_sizes = self.results_df.groupby(group_keys).size().sort_values(ascending=False)
        top_groups = group_sizes.head(self.top_n).index
        # Select all rows belonging to these top groups
        mask = self.results_df.set_index(group_keys).index.isin(top_groups)
        self.results_df_top = self.results_df[mask].reset_index(drop=True)
        # Drop the _sa_instance_state column if present
        return self.results_df_top

    def clean_and_join_dict_vals(self):
        def clean_value(val):
            if isinstance(val, int):
                return str(val)
            if isinstance(val, str) and "," in val:
                # Split, strip, sort, join with no spaces
                items = sorted(map(str.strip, val.split(",")))
                return ",".join(items)
            if isinstance(val, str):
                return val.strip()
            return str(val)
        # Clean all values
        cleaned_str = [clean_value(v) for v in self.search_criteria.values()]
        # order alphabetically
        cleaned_str.sort()
        return "SEARCH_" + "_".join(cleaned_str).replace(" ", "")

    def save_searches(self) -> None:
        """Save the search results to a CSV file."""
        # check if search directory exists, if not create it
        search_dir = self.fs.auth.parent / "search_results"
        search_dir.mkdir(parents=True, exist_ok=True)
        self.search_id = self.clean_and_join_dict_vals()
        self.search_results_fp = search_dir / f"{self.search_id}.csv"
        if self.results_df is None:
            raise ValueError("No results to save. Run do_search() first.")
        
        if not self.search_results_fp.exists():
            self.results_df.to_csv(self.search_results_fp, index=False)
            print(f"Search results saved to {self.search_results_fp}")
        else:
            print(f"Search results already exist at {self.search_results_fp}. Not overwriting.")
        
    def load_search_results(self) -> pd.DataFrame:
        """Load search results from a CSV file."""
        search_dir = self.fs.auth.parent / "search_results"
        search_fp = search_dir / f"{self.search_id}.csv"
        if search_fp.exists():
            self.results_df = pd.read_csv(search_fp)
            if '_sa_instance_state' in self.results_df.columns:
                self.results_df = self.results_df.drop(columns=['_sa_instance_state'])
            self.search_results_fp = search_fp
            self.search_results = [File(**{k: v for k, v in row.items() if k != '_sa_instance_state'}) for _, row in self.results_df.iterrows()]
            return self.results_df
        else:
            raise FileNotFoundError(f"Search results file {search_fp} not found.")
    
    def run(self) -> list[File]:
        """Perform search, sort, and return top n results as File objects. Loads from cache if available, else performs search and saves."""
        self.load_config("/Users/rt582/Library/CloudStorage/OneDrive-UniversityofCambridge/cambridge/phd/esgpullplus/search.yaml")
        # Try to load from cache if available, else perform search and save
        try:
            self.search_id = self.clean_and_join_dict_vals()
            self.load_search_results()
            print(f"Loaded search results from cache: {self.search_results_fp}")
            self.search_message('post')
        except Exception:
            self.search_message('pre')
            self.do_search()           
            self.search_message('post')
            self.sort_results_by_metadata()
            self.save_searches()
        # Always get top_n from the current results_df
        top_n_df = self.get_top_n()
        return [File(**row) for _, row in top_n_df.iterrows()]


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
            refresh_per_second=10
        )
        self.overall_task = None
        self.status_counts = {"done": 0, "skipped": 0, "failed": 0}
        self.failed_files = []  # List of (file, error_message)
        self.file_task_ids = {}  # file -> task_id
        self.file_status = {}    # file -> status string

    def __enter__(self):
        self.progress.__enter__()
        self.overall_task = self.progress.add_task("[cyan]Overall\n", total=len(self.files))
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
        self.progress.update(task_id, completed=self.progress.tasks[task_id].total)
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
        table = Table(title="Download Summary", show_header=True, header_style="bold magenta")
        table.add_column("Status", style="bold")
        table.add_column("Count", style="bold")
        table.add_row("[green]Completed[/green]", str(self.status_counts['done']))
        table.add_row("[yellow]Skipped[/yellow]", str(self.status_counts['skipped']))
        table.add_row("[red]Failed[/red]", str(self.status_counts['failed']))
        console.print(table)
        if self.failed_files:
            file, msg = self.failed_files[0]
            console.print(Panel(f"Example failed file: [bold]{file.filename}[/bold]\n[red]{msg}[/red]", title="[red]Failure Example[/red]", style="red"))


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

    def download_and_save_file(self, file, ui: DownloadProgressUI):
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
            ds = xa.open_dataset(file.url, engine="h5netcdf", chunks={"time": 2})
            if self.subset:
                missing_dims = []
                for dim in self.subset:
                    if dim not in ds.dims and dim not in ds.coords:
                        missing_dims.append(dim)
                remaining_subset_dims = {k: v for k, v in self.subset.items() if k not in missing_dims}
                if remaining_subset_dims:
                    ds = ds.isel(**remaining_subset_dims)
                else:
                    ui.set_status(file, "DL", "yellow")
            # Fine-grained progress: count dask chunks and update rich progress bar
            from dask.callbacks import Callback
            import threading
            class FileChunkProgress(Callback):
                def __init__(self, file, ui, total_chunks):
                    self.file = file
                    self.ui = ui
                    self.total_chunks = total_chunks
                    self.loaded_chunks = 0
                    self.lock = threading.Lock()
                def _start_state(self, dsk, state):
                    self.ui.update_file_progress(self.file, 0, self.total_chunks)
                def _posttask(self, key, result, dsk, state, id):
                    with self.lock:
                        self.loaded_chunks += 1
                        self.ui.update_file_progress(self.file, self.loaded_chunks)
            # Estimate total chunks
            total_chunks = 0
            for v in ds.data_vars.values():
                if hasattr(v.data, 'chunks') and v.data.chunks:
                    total_chunks += sum([len(c) for c in v.data.chunks])
            if total_chunks == 0:
                total_chunks = 1
            with dask.config.set(scheduler='threads'):
                with FileChunkProgress(file, ui, total_chunks):
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
                ds = xr.open_dataset(file.url, engine="h5netcdf", chunks={"time": 2})
                return (file, ds, None)
            except Exception as e:
                return (file, None, e)
        with DownloadProgressUI(files) as ui:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = list(executor.map(open_ds, files))
            datasets = []
            for file, ds, err in results:
                if err is not None or ds is None:
                    ui.set_status(file, "FAIL", "red")
                    ui.add_failed(file, f"FAIL {file.filename}: {err}")
                    ui.complete_file(file)
                else:
                    ui.set_status(file, "DL", "cyan")
                    datasets.append((file, ds))
            # Step 2: (Optional) Subset
            for i, (file, ds) in enumerate(datasets):
                if self.subset:
                    missing_dims = [dim for dim in self.subset if dim not in ds.dims and dim not in ds.coords]
                    remaining_subset_dims = {k: v for k, v in self.subset.items() if k not in missing_dims}
                    if remaining_subset_dims:
                        ds = ds.isel(**remaining_subset_dims)
                        datasets[i] = (file, ds)
            # Step 3: Trigger parallel loading of all datasets
            # Setup per-file chunk progress
            from dask.callbacks import Callback
            import threading
            class FileChunkProgress(Callback):
                def __init__(self, file, ui, total_chunks):
                    self.file = file
                    self.ui = ui
                    self.total_chunks = total_chunks
                    self.loaded_chunks = 0
                    self.lock = threading.Lock()
                def _start_state(self, dsk, state):
                    self.ui.update_file_progress(self.file, 0, self.total_chunks)
                def _posttask(self, key, result, dsk, state, id):
                    with self.lock:
                        self.loaded_chunks += 1
                        self.ui.update_file_progress(self.file, self.loaded_chunks)
            # Build all chunk progress callbacks
            callbacks = []
            load_calls = []
            for file, ds in datasets:
                # Estimate total chunks
                total_chunks = 0
                for v in ds.data_vars.values():
                    if hasattr(v.data, 'chunks') and v.data.chunks:
                        total_chunks += sum([len(c) for c in v.data.chunks])
                if total_chunks == 0:
                    total_chunks = 1
                callbacks.append(FileChunkProgress(file, ui, total_chunks))
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
        table = Table(title="Download Summary", show_header=True, header_style="bold magenta")
        table.add_column("Status", style="bold")
        table.add_column("Count", style="bold")
        table.add_row("[green]Completed[/green]", str(self.status_counts['done']))
        table.add_row("[yellow]Skipped[/yellow]", str(self.status_counts['skipped']))
        table.add_row("[red]Failed[/red]", str(self.status_counts['failed']))
        console.print(Panel(table, title="[cyan]Download Complete", border_style="cyan"))

    def run(self):
        file_str = 'files' if len(self.files) > 1 else 'file'
        print(f"Attempting download of {len(self.files)} {file_str}...\n")
        # Use new parallel method for >1 file
        # if len(self.files) > 1:
        #     self.download_all_parallel()
        # else:
        with DownloadProgressUI(self.files) as ui:
            def wrapped_download(file):
                return self.download_and_save_file(file, ui)
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    list(executor.map(wrapped_download, self.files))
            except KeyboardInterrupt:
                print("\nDownload interrupted by user. Shutting down workers...")
                executor.shutdown(wait=False, cancel_futures=True)
            finally:
                ui.print_summary()


class RegridderManager:
    # TODO: getting a esmf warning when trying to regrid the same (deleted file): have to restart code
    def __init__(self, fs, ds, target_res=(1,1), periodic=True, verbose=1):
        """
        ds: xarray.Dataset with native curvilinear ocean grid
        target_res: resolution as (lon_res, lat_res)
        weight_dir: optional path to save/load weights
        """
        self.fs = fs
        self.ds = ds.load()
        self.success_count = 0
        self.fail_count = 0
        self.periodic = periodic
        self.target_res = target_res
        self.varname = self._get_varname()
        self.ds = self._standardize_dims()
        self.ds = self._standardize_coords()
        self.ds = self._standardise_lon_limits()
        self.weight_dir = self.fs.data / "xesmf_regrid_weights"
        self.weight_dir.mkdir(exist_ok=True)
        self.regridder = self._get_or_create_regridder()
    
    
    def estimate_target_resolution(self):
        # TODO
        """
        Estimate target resolution based on the dataset's native grid.
        If target_res is not provided, use the native resolution of the dataset.
        """
        if self.target_res is not None:
            return self.target_res
        lon = self.ds['lon'] if 'lon' in self.ds.coords else self.ds['longitude']
        lat = self.ds['lat'] if 'lat' in self.ds.coords else self.ds['latitude']
        lon_res = np.abs(lon[1] - lon[0])

    def _get_varname(self):
        varname = None
        for v in self.ds.data_vars:
            if not any(sub in v.lower() for sub in ['bnds', 'vertices']):
                varname = v
                break
        if varname is None:
            if verbose == 1:
                print(f"No suitable variable found in {ncfile}")
            return None
        return varname

    def _standardize_dims(self):
        # Robustly assign 'x' to longitude and 'y' to latitude, even if i/j are swapped
        dim_map = {}
        dims = list(self.ds.dims)
        # If both i and j are present, decide which is x (lon) and which is y (lat) by shape
        if 'i' in dims and 'j' in dims:
            i_len = self.ds.sizes['i']
            j_len = self.ds.sizes['j']
            # Longitude usually has more points than latitude
            if i_len > j_len:
                dim_map['i'] = 'x'  # i is longitude
                dim_map['j'] = 'y'  # j is latitude
            else:
                dim_map['i'] = 'y'  # i is latitude
                dim_map['j'] = 'x'  # j is longitude
        else:
            if 'i' in dims: dim_map['i'] = 'y'
            if 'j' in dims: dim_map['j'] = 'x'
        return self.ds.rename_dims(dim_map)

    def _standardize_coords(self):
        # Ensure 'lat' and 'lon' are present and correctly named
        self.ds = self.ds.rename({'latitude': 'lat'}) if 'latitude' in self.ds.coords and 'lat' not in self.ds.coords else self.ds
        self.ds = self.ds.rename({'longitude': 'lon'}) if 'longitude' in self.ds.coords and 'lon' not in self.ds.coords else self.ds
        return self.ds

    def _make_grid_in(self):
        lons = np.ascontiguousarray(self.ds['lon'].values)
        lats = np.ascontiguousarray(self.ds['lat'].values)
        
        if lons.ndim == 1 and lats.ndim == 1:
            return xa.Dataset({'lon': (['x'], lons), 'lat': (['y'], lats)})
        elif lons.ndim == 2 and lats.ndim == 2:
            return xa.Dataset({'lon': (['x', 'y'], lons), 'lat': (['x', 'y'], lats)})   # TODO: I think this is still sometimes failing (different for different files)
        else:
            raise ValueError(f"Unsupported dimensions: lon: {lons.ndim}, lat: {lats.ndim}. Expected 1D or 2D array.")

    def _standardise_lon_limits(self):
        lon = self.ds['lon'] if 'lon' in self.ds.coords else self.ds['longitude']

        # If all longitudes are >= 0, shift to -180..180/360
        if np.all(lon.values >= 0):
            lon = ((lon - 180) % 360) - 180
            # Also update the dataset so downstream code uses shifted lons
            if 'lon' in self.ds.coords:
                self.ds = self.ds.assign_coords(lon=lon)
            else:
                self.ds = self.ds.assign_coords(longitude=lon)
        return self.ds

    def _make_grid_out(self):
        lon_res, lat_res = self.target_res
        
        target_lon = np.arange(-180, 180 + lon_res, lon_res)
        target_lat = np.arange(-90, 90 + lat_res, lat_res)
        return xa.Dataset({'lon': (['lon'], target_lon), 'lat': (['lat'], target_lat)})

    def _weights_filename(self):
        # Hash the shape of input grid to ensure reuse
        id_str = f"{self.ds['lon'].shape}-{self.target_res}"
        hex_hash = hashlib.md5(id_str.encode()).hexdigest()
        
        return self.weight_dir / f"regrid_weights_{hex_hash}.nc"

    def _get_or_create_regridder(self):
        grid_in = self._make_grid_in()
        grid_out = self._make_grid_out()
        weights_path = self._weights_filename()

        if weights_path.exists():
            return xe.Regridder(grid_in, grid_out,
                                method='bilinear',
                                periodic=self.periodic,
                                filename=weights_path,
                                reuse_weights=True)
        else:
            return xe.Regridder(grid_in, grid_out,
                                method='bilinear',
                                periodic=self.periodic,
                                ignore_degenerate=True,
                                filename=weights_path)

    def _trim_unnecessary_vals(self):
        # remove i,j,latitude, longitude coords
        coords_to_remove = ['i', 'j', 'latitude', 'longitude']
        for coord in coords_to_remove:
            if coord in self.ds.coords:
                self.ds = self.ds.drop_vars(coord)
        # remove any bounds data variables
        bounds_vars = [v for v in self.ds.data_vars if 'bnds' in v.lower() or 'vertices' in v.lower()]
        for var in bounds_vars:
            if var in self.ds.data_vars:
                self.ds = self.ds.drop_vars(var)
        return self.ds

    def regrid(self, time_index=None):
        data = self.ds[self.varname]
        if time_index is not None:
            data = data.isel(time=time_index)

        data = data.where(np.isfinite(data), drop=False)
        regridded_data = self.regridder(data)
        self.ds[self.varname] = regridded_data
        self.ds = self._trim_unnecessary_vals()
        return self.ds

    @staticmethod
    def regrid_all_files_in_tree(watch_dir, subdir="reprojected", delete_original=False, verbose=True, fs=None, max_workers=1):
        """
        Scan all subdirectories of watch_dir, create a 'reprojected' folder in each, and regrid all new .nc files in parallel.
        Only processes files that do not already have a regridded version.
        """

        watch_dir = Path(watch_dir)
        files_to_regrid = []
        for ncfile in watch_dir.rglob("*.nc"):
            if 'reprojected' in str(ncfile):
                continue
            if 'regrid_weights' in str(ncfile):
                continue
            parent_dir = ncfile.parent
            out_dir = parent_dir / subdir
            out_dir.mkdir(exist_ok=True)
            out_file = out_dir / ncfile.name
            if out_file.exists():
                continue
            files_to_regrid.append((ncfile, out_file))

        from rich.console import Console
        from rich.panel import Panel
        from rich.text import Text
        console = Console()

        def handle_file(args):
            ncfile, out_file = args
            try:
                ds = xa.open_dataset(ncfile)
                regrid_mgr = RegridderManager(fs=fs, ds=ds)
                out_ds = ds.copy()
                out_ds = regrid_mgr.regrid()
                out_ds.to_netcdf(out_file)
                if verbose:
                    short_path = str(Path(*ncfile.parts[-6:]))
                    console.print(f"[green]Regridded:[/green] {short_path}")
                    # disappear after 2 seconds
                if delete_original:
                    ncfile.unlink()
            except Exception as e:
                short_path = str(Path(*ncfile.parts[-6:]))
                console.print(f"[red]Failed to regrid:[/red] {short_path} [red]{e}[/red]")

        if files_to_regrid:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(handle_file, files_to_regrid)
            # TODO: Print summary panel at the end. Would require a refactor of the regrid_all_files_in_tree manager to be more like the Download class
            # console.print(Panel(f"[green]Successful:[/green] {self.success_count}\n[red]Failed:[/red] {self.fail_count}", title="Regridding Summary", style="bold blue"))
        else:
            if verbose:
                console.print("[yellow]\nNo new files to regrid.[/yellow]")


CRITERIA_FP = read_yaml('/Users/rt582/Library/CloudStorage/OneDrive-UniversityofCambridge/cambridge/phd/esgpullplus/search.yaml')
SEARCH_CRITERIA_CONFIG = CRITERIA_FP.get('search_criteria', {})
META_CRITERIA_CONFIG = CRITERIA_FP.get('meta_criteria', {})
META_CRITERIA_CONFIG['limit'] = META_CRITERIA_CONFIG['limit']

API = EsgpullAPI()

    
def main():
    # load configuration

    files = SearchResults(search_criteria=SEARCH_CRITERIA_CONFIG, meta_criteria=META_CRITERIA_CONFIG).run()
    
    # # start processor running, if specified by commandline argument
    # if META_CRITERIA_CONFIG.get('process', False):
    #     asyncio.run(watch_and_reproject(watch_dir=api.esg.fs.data))
    
    # TODO: group results in such a way that you get a coherent set of files (e.g. all the relevant timesteps for a particular source)
    DownloadSubset(
        files=files,
        fs=API.esg.fs,
        output_dir=META_CRITERIA_CONFIG.get('output_dir', None),  # Optional output directory
        subset=META_CRITERIA_CONFIG.get('subset'),  # Optional subset criteria for xarray
        max_workers=META_CRITERIA_CONFIG.get('max_workers', 16)  # Default to 16 workers
    ).run()
    
    # run regridding on all new files in the watch directory
    RegridderManager.regrid_all_files_in_tree(watch_dir=API.esg.fs.data, fs=API.esg.fs)
    
if __name__ == "__main__":
#     print('Starting async watcher...')
#     watcher = AsyncRegridWatcher(watch_dir=API.esg.fs.data)
#     watcher_thread = threading.Thread(target=lambda: asyncio.run(watcher.start()), daemon=True)
#     watcher_thread.start()
    main()
    
    
    


# class AsyncRegridWatcher:
#     def __init__(self, watch_dir, subdir="reprojected", poll_interval=10, delete_original=False, verbose=True, stop_after_idle=60, fs=None):
#         from pathlib import Path
#         self.watch_dir = Path(watch_dir)
#         self.subdir = subdir
#         self.poll_interval = poll_interval
#         self.delete_original = delete_original
#         self.verbose = verbose
#         self.stop_after_idle = stop_after_idle
#         self.fs = fs
#         self.processed = set()
#         self.idle_time = 0

#     async def start(self):
#         import asyncio
#         from concurrent.futures import ThreadPoolExecutor
#         from pathlib import Path
#         try:
#             while True:
#                 found_new = False
#                 files_to_regrid = []
#                 # Recursively find all .nc files in all subdirectories
#                 for ncfile in self.watch_dir.rglob("*.nc"):
#                     parent_dir = ncfile.parent
#                     out_dir = parent_dir / self.subdir
#                     out_dir.mkdir(exist_ok=True)
#                     out_file = out_dir / ncfile.name
#                     if ncfile in self.processed or out_file.exists():
#                         continue
#                     # Check file is accessible (not being written to)
#                     try:
#                         with open(ncfile, "rb") as f:
#                             f.read(1024)
#                         files_to_regrid.append((ncfile, out_file))
#                     except Exception:
#                         continue  # File is still being written
#                 if files_to_regrid:
#                     found_new = True
#                     def sync_handle(args):
#                         ncfile, out_file = args
#                         try:
#                             import asyncio
#                             loop = asyncio.new_event_loop()
#                             asyncio.set_event_loop(loop)
#                             loop.run_until_complete(self.handle_new_file(ncfile, out_file))
#                             loop.close()
#                         except Exception as e:
#                             print(f"Failed to regrid {ncfile}: {e}")
#                     with ThreadPoolExecutor() as executor:
#                         executor.map(sync_handle, files_to_regrid)
#                     for ncfile, _ in files_to_regrid:
#                         self.processed.add(ncfile)
#                 if not found_new:
#                     self.idle_time += self.poll_interval
#                 else:
#                     self.idle_time = 0
#                 if self.stop_after_idle is not None and self.idle_time >= self.stop_after_idle:
#                     if self.verbose:
#                         print("No new files detected. Exiting watcher.")
#                     break
#                 await asyncio.sleep(self.poll_interval)
#         except (KeyboardInterrupt, asyncio.CancelledError):
#             print("\nWatcher interrupted by user. Exiting cleanly.")
#             return

#     async def handle_new_file(self, ncfile, out_file):
#         import xarray as xa
#         from esgpull.custom import RegridderManager
#         ds = xa.open_dataset(ncfile)
#         varname = self.pick_varname(ds)
#         if varname is None:
#             if self.verbose:
#                 print(f"No suitable variable found in {ncfile}")
#             return
#         regrid_mgr = RegridderManager(fs=self.fs, ds=ds)
#         out_ds = ds.copy()
#         out_ds[varname] = regrid_mgr.regrid(varname)
#         out_ds.to_netcdf(out_file)
#         if self.verbose:
#             print(f"Regridded: {ncfile} -> {out_file}")
#         if self.delete_original:
#             ncfile.unlink()

#     def pick_varname(self, ds):
#         # Pick the first non-coord, non-bounds variable
#         for v in ds.data_vars:
#             if not any(x in v.lower() for x in ["bound", "vertex", "mask"]):
#                 return v
#         return None