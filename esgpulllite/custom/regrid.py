#!/usr/bin/env python3
# general
import numpy as np
import hashlib
from pathlib import Path
import tempfile
import re
import time

# spatial
import xarray as xa
import xesmf as xe
from cdo import Cdo

# parallel
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# import multiprocessing as mp
import os
import gc

# rich
from rich.console import Console

# custom
from reefshift.dataloading import processdata


def regrid_all_files_in_tree(
    watch_dir,
    subdir="reprojected",
    delete_original=False,
    fs=None,
    max_workers=1,
    target_res=None,
    periodic=True,
    reuse_weights=True,
):
    """
    Scan all subdirectories of watch_dir, create a 'reprojected' folder in each, and regrid all new .nc files
    in parallel. Only processes files that do not already have a regridded version.

    Parameters
    ----------
    watch_dir : str or Path
        Directory to scan for NetCDF files
    subdir : str
        Subdirectory name to create for regridded files
    delete_original : bool
        Whether to delete original files after successful regridding
    fs : optional
        File system object to pass to RegridderManager
    max_workers : int
        Maximum number of parallel workers
    target_res : tuple
        Target resolution as (lon_res, lat_res)
    periodic : bool
        Whether to use periodic boundary conditions
    reuse_weights : bool
        Whether to reuse weight files for faster processing
    """
    watch_dir = Path(watch_dir)
    files_to_regrid = []
    skipped = 0
    # Find all NetCDF files that need regridding
    for ncfile in watch_dir.rglob("*.nc*"):
        if "reprojected" in str(ncfile) or "regrid_weights" in str(ncfile):
            continue
        parent_dir = ncfile.parent
        out_dir = parent_dir / subdir
        out_dir.mkdir(exist_ok=True)
        out_file = out_dir / ncfile.name
        if out_file.exists():
            skipped += 1
            continue
        files_to_regrid.append((ncfile, out_file))

    console = Console()
    success_count = 0
    fail_count = 0

    start_time = time.localtime()
    # start timer
    console.print(f":clock3: START: {time.strftime('%Y-%m-%d %H:%M:%S', start_time)}\n")

    def handle_file(args):
        nonlocal success_count, fail_count
        ncfile, out_file = args
        try:
            # Load dataset
            ds = xa.open_dataset(ncfile)
            # rename latitude, longitude to lat, lon
            ds = ds.rename({"latitude": "lat", "longitude": "lon"})
            short_path = str(Path(*ncfile.parts[-6:]))

            # Create regridder manager and use unified regrid_ds method
            regrid_mgr = RegridderManager(
                ds=ds, fs=fs, target_res=target_res, periodic=periodic
            )

            # Use the unified regrid_ds method with weight reuse
            regridded_ds = processdata.process_xa_d(
                regrid_mgr.regrid_ds(ds, reuse_weights=reuse_weights)
            )

            # Save to output file
            regridded_ds.to_netcdf(out_file)

            # Determine regridding method for logging
            method_used = "CDO" if "ncells" in ds.dims else "xESMF"
            console.print(f"[green]Regridded ({method_used}):[/green] {short_path}")

            success_count += 1

            # Clean up original file if requested
            if delete_original:
                ncfile.unlink()

        except Exception as e:
            console.print(f"[red]Failed to regrid:[/red] {short_path} [red]{e}[/red]")
            fail_count += 1

    if files_to_regrid:
        console.print(
            f"[blue]Processing {len(files_to_regrid)} files with {max_workers} worker(s)...[/blue]"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(handle_file, files_to_regrid)

        # Print summary
        console.print("[bold blue]Regridding Summary:[/bold blue]")
        console.print(f"[green]Successful:[/green] {success_count}")
        console.print(f"[red]Failed:[/red] {fail_count}")
        console.print(f"[white]Skipped:[/white] {skipped}")
        console.print(f"[blue]Total processed:[/blue] {success_count + fail_count}")

    else:
        console.print("[yellow]No new files to regrid.[/yellow]")

    # end timer
    end_time = time.localtime()
    console.print(f":clock3: END: {time.strftime('%Y-%m-%d %H:%M:%S', end_time)}\n")
    duration = time.mktime(end_time) - time.mktime(start_time)
    mins, secs = divmod(int(duration), 60)
    hours, mins = divmod(mins, 60)
    console.print(f":clock3: DURATION: {hours:02d}:{mins:02d}:{secs:02d}\n")


class RegridderManager:
    # TODO: getting a esmf warning when trying to regrid the same (deleted file): have to restart code
    def __init__(self, ds=None, fs=None, target_res=None, periodic=True):
        """
        ds: xarray.Dataset with native curvilinear ocean grid
        target_res: resolution as (lon_res, lat_res)
        weight_dir: optional path to save/load weights
        """
        self.fs = fs
        self.ds = ds.load()
        self.success_count = 0  # TODO: track successful regriddings
        self.fail_count = 0
        self.periodic = periodic
        self.target_res = self.get_target_resolution() if not target_res else target_res
        self.varname = self._get_varname()
        self.ds = self._convert_dataarray_to_dataset(self.ds)
        self.ds = self._standardize_dims()
        self.ds = self._standardize_coords()
        self.ds = self._standardise_lon_limits()
        self.weight_dir = self.fs.data.parent / "xesmf_regrid_weights" if fs else None
        self.weight_dir.mkdir(exist_ok=True) if self.weight_dir else None

    def _convert_dataarray_to_dataset(self, ds):
        """
        Convert DataArray to Dataset if necessary.
        If ds is a DataArray, convert it to a Dataset with the variable name as the only data variable.
        """
        if isinstance(ds, xa.DataArray):
            return xa.Dataset({self.varname: ds})
        elif isinstance(ds, xa.Dataset):
            return ds
        else:
            raise ValueError("Input must be an xarray.DataArray or xarray.Dataset")

    def get_target_resolution(self):
        """
        Estimate target resolution based on the dataset's native grid.
        If target_res is not provided, use the native resolution of the dataset.
        """
        res = self.ds.attrs.get("nominal_resolution", None)
        if res is None:
            # Try to infer resolution from coordinates
            res = self._infer_resolution_from_coords()
        else:
            res = str(res).lower().replace(" ", "")
            if m := re.match(r"([\d.]+)\s*km", res):
                val = m.group(1)
                if val == "10":
                    res = (0.1, 0.1)
                elif val == "25":
                    res = (0.25, 0.25)
                elif val == "50":
                    res = (0.5, 0.5)
                else:
                    res = float(val)
            elif m := re.match(r"([\d.]+)x([\d.]+)degree", res):
                res = (float(m.group(1)), float(m.group(2)))
            else:
                print(f"Could not parse resolution from {res}, using default (1, 1)")
                res = (1, 1)
        # assume res is a tuple (lon_res, lat_res) and the same
        if isinstance(res, (int, float)):
            res = (res, res)
        return res

    def _infer_resolution_from_coords(self):
        """
        Infer resolution from the dataset's coordinates.
        Assumes coordinates are evenly spaced.
        Returns (1, 1) if unable to infer.
        """
        try:
            lon = self.ds["lon"] if "lon" in self.ds.coords else self.ds["longitude"]
            lat = self.ds["lat"] if "lat" in self.ds.coords else self.ds["latitude"]
            if lon.ndim == 1 and lat.ndim == 1 and len(lon) > 1 and len(lat) > 1:
                lon_res = float(np.abs(lon[1] - lon[0]))
                lat_res = float(np.abs(lat[1] - lat[0]))
            elif (
                lon.ndim == 2
                and lat.ndim == 2
                and lon.shape[0] > 1
                and lon.shape[1] > 1
            ):
                lon_res = float(np.abs(lon[:, 1] - lon[:, 0]).mean())
                lat_res = float(np.abs(lat[1, :] - lat[0, :]).mean())
            else:
                # Fallback if dimensions are not as expected
                return (1, 1)
            # round lon_res and lat_res to closest quarter of a degree
            lon_res = abs(round(lon_res * 4) / 4)
            lat_res = abs(round(lat_res * 4) / 4)
            return (lon_res, lat_res)
        except Exception:
            return (1, 1)

    def _get_varname(self, ncfile=None):
        if isinstance(self.ds, xa.DataArray):
            try:
                return self.ds.name
            except AttributeError:
                print("DataArray has no name, cannot determine variable name.")
                return None
        varname = None
        for v in self.ds.data_vars:
            if not any(sub in v.lower() for sub in ["bnds", "vertices"]):
                varname = v
                break
        if varname is None:
            print(f"No suitable variable found in {ncfile}")
            return None
        return varname

    def _standardize_dims(self):
        # Robustly assign 'x' to longitude and 'y' to latitude, even if i/j are swapped
        dim_map = {}
        dims = list(self.ds.dims)
        # If both i and j are present, decide which is x (lon) and which is y (lat) by shape
        if "i" in dims and "j" in dims:
            i_len = self.ds.sizes["i"]
            j_len = self.ds.sizes["j"]
            # Longitude usually has more points than latitude
            if i_len > j_len:
                dim_map["i"] = "x"  # i is longitude
                dim_map["j"] = "y"  # j is latitude
            else:
                dim_map["i"] = "y"  # i is latitude
                dim_map["j"] = "x"  # j is longitude
        else:
            if "i" in dims:
                dim_map["i"] = "y"
            if "j" in dims:
                dim_map["j"] = "x"
        self.ds = self.ds.rename_dims(dim_map)
        # Ensure new dimension names are indexed
        # for old_dim, new_dim in dim_map.items():
        #     if (
        #         old_dim in self.ds.dims
        #         and new_dim in self.ds.dims
        #         and new_dim not in self.ds.indexes
        #     ):
        #         self.ds = self.ds.swap_dims({old_dim: new_dim})
        self.ds = self.ds.rename(dim_map)
        return self.ds

    def _standardize_coords(self):
        # Ensure 'lat' and 'lon' are present and correctly named
        self.ds = (
            self.ds.rename({"latitude": "lat"})
            if "latitude" in self.ds.coords and "lat" not in self.ds.coords
            else self.ds
        )
        self.ds = (
            self.ds.rename({"longitude": "lon"})
            if "longitude" in self.ds.coords and "lon" not in self.ds.coords
            else self.ds
        )

        return self.ds

    def _make_grid_in(self):
        lons = self.ds["lon"].values
        lats = self.ds["lat"].values

        if lons.ndim == 2 and lons.shape[1] > lons.shape[0]:
            lons = lons.T
            lats = lats.T

        lons = np.asfortranarray(lons)
        lats = np.asfortranarray(lats)

        if lons.ndim == 1 and lats.ndim == 1:
            return xa.Dataset({"lon": (["x"], lons), "lat": (["y"], lats)})
        elif lons.ndim == 2 and lats.ndim == 2:
            return xa.Dataset(
                {"lon": (["x", "y"], lons), "lat": (["x", "y"], lats)}
            )  # TODO: I think this is still sometimes failing (different for different files)
        else:
            raise ValueError(
                f"Unsupported dimensions: lon: {lons.ndim}, lat: {lats.ndim}. Expected 1D or 2D array."
            )

    def _standardise_lon_limits(self):
        lon = self.ds["lon"] if "lon" in self.ds.coords else self.ds["longitude"]

        # If all longitudes are >= 0, shift to -180..180/360
        if np.all(lon.values >= 0):
            lon = ((lon - 180) % 360) - 180
            # Also update the dataset so downstream code uses shifted lons
            if "lon" in self.ds.coords:
                self.ds = self.ds.assign_coords(lon=lon)
            else:
                self.ds = self.ds.assign_coords(longitude=lon)
        return self.ds

    def _make_grid_out(self):
        lon_res, lat_res = self.target_res

        target_lon = np.arange(-180, 180 + lon_res, lon_res)
        target_lat = np.arange(-90, 90 + lat_res, lat_res)
        return xa.Dataset({"lon": (["lon"], target_lon), "lat": (["lat"], target_lat)})

    def _weights_filename(self):
        # Hash the shape of input grid to ensure reuse
        id_str = f"{self.ds['lon'].shape}-{self.target_res}"
        hex_hash = hashlib.md5(id_str.encode()).hexdigest()

        return self.weight_dir / f"regrid_weights_{hex_hash}.nc"

    def _get_or_create_regridder(self):
        grid_in = self._make_grid_in()
        grid_out = self._make_grid_out()
        weights_path = self._weights_filename() if self.weight_dir else None

        if weights_path and weights_path.exists():
            return xe.Regridder(
                grid_in,
                grid_out,
                method="bilinear",
                periodic=self.periodic,
                filename=weights_path,
                reuse_weights=True,
            )
        else:
            return xe.Regridder(
                grid_in,
                grid_out,
                method="bilinear",
                periodic=self.periodic,
                ignore_degenerate=True,
                # filename=weights_path,
            )

    def _trim_unnecessary_vals(self):
        # remove i,j,latitude, longitude coords
        coords_to_remove = ["i", "j", "latitude", "longitude"]
        for coord in coords_to_remove:
            if coord in self.ds.coords:
                self.ds = self.ds.drop_vars(coord)
        # remove any bounds data variables
        bounds_vars = [
            v
            for v in self.ds.data_vars
            if "bnds" in v.lower() or "vertices" in v.lower()
        ]
        for var in bounds_vars:
            if var in self.ds.data_vars:
                self.ds = self.ds.drop_vars(var)
        return self.ds

    def _trim_unnecessary_vals_from_ds(self, ds):
        """
        Remove unnecessary coordinates and variables from a dataset.
        This is a static version of _trim_unnecessary_vals that works on any dataset.
        """
        # remove i,j,latitude, longitude coords
        coords_to_remove = ["i", "j", "latitude", "longitude"]
        for coord in coords_to_remove:
            if coord in ds.coords:
                ds = ds.drop_vars(coord)
        # remove any bounds data variables
        bounds_vars = [
            v for v in ds.data_vars if "bnds" in v.lower() or "vertices" in v.lower()
        ]
        for var in bounds_vars:
            if var in ds.data_vars:
                ds = ds.drop_vars(var)
        return ds

    def _cdo_weights_filename(self, grid_type: str, xsize: int, ysize: int):
        """Generate a unique filename for CDO weights based on grid parameters."""
        if not self.weight_dir:
            return None

        # Create unique identifier for this grid configuration
        grid_id = (
            f"cdo_{grid_type}_{xsize}x{ysize}_{self.ds.dims.get('ncells', 'unknown')}"
        )
        hex_hash = hashlib.md5(grid_id.encode()).hexdigest()

        return self.weight_dir / f"cdo_weights_{hex_hash}.nc"

    def regrid_with_cdo(
        self,
        ds: xa.Dataset = None,
        grid_type: str = "remapcon",  # options: remapnn, remapbil, remapcon
        xsize: int = 360,
        ysize: int = 180,
        xfirst: float = -179.5,
        xinc: float = 1.0,
        yfirst: float = -89.5,
        yinc: float = 1.0,
        reuse_weights: bool = True,
    ) -> xa.Dataset:
        """
        Regrid an unstructured xarray.Dataset to a regular lat-lon grid using CDO.

        Now supports weight reuse for faster subsequent regridding operations.

        Parameters
        ----------
        ds : xa.Dataset
            Input dataset with unstructured grid (e.g., with lat_bnds/lon_bnds).
        grid_type : str
            CDO regridding method: 'remapnn', 'remapbil', or 'remapcon'.
        xsize, ysize : int
            Output grid dimensions (longitude × latitude).
        xfirst, yfirst : float
            Starting coordinates for the grid.
        xinc, yinc : float
            Grid spacing in degrees.
        reuse_weights : bool
            Whether to generate and reuse weight files for faster processing.

        Returns
        -------
        xa.Dataset
            Regridded dataset with regular lat-lon grid.
        """
        cdo = Cdo()
        ds = ds if ds is not None else self.ds

        # Get weights filename if reuse is enabled
        weights_path = None
        if reuse_weights:
            weights_path = self._cdo_weights_filename(grid_type, xsize, ysize)

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.nc"
            output_path = Path(tmpdir) / "output.nc"
            gridfile_path = Path(tmpdir) / "grid.txt"

            # Save dataset to disk as a temporary NetCDF file
            ds.to_netcdf(input_path)

            # Create CDO target grid description
            with open(gridfile_path, "w") as f:
                f.write(
                    f"""gridtype = lonlat
xsize = {xsize}
ysize = {ysize}
xfirst = {xfirst}
xinc = {xinc}
yfirst = {yfirst}
yinc = {yinc}
"""
                )

            # Check if we can reuse existing weights
            if weights_path and weights_path.exists():
                # Use existing weights for faster regridding
                print(f"Reusing CDO weights: {weights_path.name}")
                cdo.remap(
                    str(gridfile_path),
                    str(weights_path),
                    input=str(input_path),
                    output=str(output_path),
                )
            else:
                # Generate new weights (and save them if enabled)
                if weights_path:
                    # Generate weights file for reuse
                    print(f"Generating new CDO weights: {weights_path.name}")
                    temp_weights = Path(tmpdir) / "temp_weights.nc"

                    # First, generate the weights
                    getattr(cdo, f"gen{grid_type[5:]}")(  # remove "remap" prefix
                        str(gridfile_path),
                        input=str(input_path),
                        output=str(temp_weights),
                    )

                    # Copy weights to permanent location
                    import shutil

                    shutil.copy2(temp_weights, weights_path)

                    # Now use the weights for actual regridding
                    cdo.remap(
                        str(gridfile_path),
                        str(weights_path),
                        input=str(input_path),
                        output=str(output_path),
                    )
                else:
                    # Direct regridding without weight saving
                    print("CDO regridding without weight caching")
                    getattr(cdo, grid_type)(
                        str(gridfile_path),
                        input=str(input_path),
                        output=str(output_path),
                    )

            # Load result back into xarray
            ds_out = xa.open_dataset(output_path)

        return ds_out

    def regrid_ds(
        self, ds: xa.Dataset = None, reuse_weights: bool = True
    ) -> xa.Dataset:
        """
        Regrid a dataset using the current regridder.
        This is a convenience method to regrid without needing to manage the regridder directly.

        Parameters
        ----------
        ds : xa.Dataset, optional
            Dataset to regrid. If None, uses self.ds
        reuse_weights : bool
            Whether to reuse weight files for faster CDO regridding
        """
        # Update the instance dataset
        self.ds = ds if ds is not None else self.ds
        self.varname = self._get_varname()

        if self.varname is None:
            raise ValueError("No suitable variable found for regridding")

        if "ncells" in self.ds.dims:
            # unstructured grid, use CDO with weight reuse
            lon_res, lat_res = self.target_res
            xsize = int(360 / lon_res)
            ysize = int(180 / lat_res)
            out_ds = self.regrid_with_cdo(
                ds, xsize=xsize, ysize=ysize, reuse_weights=reuse_weights
            )
            return out_ds
        else:
            # Regular grid, use xESMF (already supports weight reuse)
            data = self.ds[self.varname]
            data = data.where(np.isfinite(data), drop=False)
            data.values[:] = np.ascontiguousarray(data.values)
            self.regridder = self._get_or_create_regridder()
            regridded_data = self.regridder(data)

            # Create output dataset
            out_ds = self.ds.copy()
            out_ds[self.varname] = regridded_data
            out_ds = self._trim_unnecessary_vals_from_ds(out_ds)
            return out_ds


def regrid_files_safely_parallel(
    watch_dir,
    subdir="reprojected",
    delete_original=False,
    fs=None,
    max_workers=1,
    target_res=None,
    periodic=True,
    reuse_weights=True,
    chunk_size=1,
):
    """
    Safe parallel regridding that avoids ESMF rc=545 errors by using process-based parallelization
    instead of thread-based, and adding proper ESMF environment isolation.

    Parameters
    ----------
    watch_dir : str or Path
        Directory to scan for NetCDF files
    subdir : str
        Subdirectory name to create for regridded files
    delete_original : bool
        Whether to delete original files after successful regridding
    fs : optional
        File system object to pass to RegridderManager
    max_workers : int
        Maximum number of parallel workers (processes, not threads)
    target_res : tuple
        Target resolution as (lon_res, lat_res)
    periodic : bool
        Whether to use periodic boundary conditions
    reuse_weights : bool
        Whether to reuse weight files for faster processing
    chunk_size : int
        Number of files to process per worker at once
    """

    watch_dir = Path(watch_dir)
    files_to_regrid = []
    skipped = 0

    # Find all NetCDF files that need regridding
    for ncfile in watch_dir.rglob("*.nc*"):
        if "reprojected" in str(ncfile) or "regrid_weights" in str(ncfile):
            continue
        parent_dir = ncfile.parent
        out_dir = parent_dir / subdir
        out_dir.mkdir(exist_ok=True)
        out_file = out_dir / ncfile.name
        if out_file.exists():
            skipped += 1
            continue
        files_to_regrid.append((ncfile, out_file))

    console = Console()
    start_time = time.localtime()
    console.print(f":clock3: START: {time.strftime('%Y-%m-%d %H:%M:%S', start_time)}\n")

    if not files_to_regrid:
        console.print("[yellow]No new files to regrid.[/yellow]")
        return

    console.print(f"[blue]Found {len(files_to_regrid)} files to regrid[/blue]")
    console.print(
        f"[blue]Using {max_workers} process(es) with chunk size {chunk_size}[/blue]"
    )

    # Create chunks of files to process
    file_chunks = [
        files_to_regrid[i : i + chunk_size]
        for i in range(0, len(files_to_regrid), chunk_size)
    ]

    success_count = 0
    fail_count = 0

    if max_workers == 1:
        # Single-threaded processing (safest for ESMF)
        for chunk in file_chunks:
            chunk_success, chunk_fail = _process_file_chunk_safe(
                chunk, fs, target_res, periodic, reuse_weights, delete_original
            )
            success_count += chunk_success
            fail_count += chunk_fail
    else:
        # Multi-process parallelization (safer than threading for ESMF)
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit chunks to workers
            future_to_chunk = {
                executor.submit(
                    _process_file_chunk_safe,
                    chunk,
                    fs,
                    target_res,
                    periodic,
                    reuse_weights,
                    delete_original,
                ): chunk
                for chunk in file_chunks
            }

            # Collect results
            for future in future_to_chunk:
                try:
                    chunk_success, chunk_fail = future.result()
                    success_count += chunk_success
                    fail_count += chunk_fail
                except Exception as e:
                    console.print(f"[red]Chunk processing failed: {e}[/red]")
                    fail_count += len(future_to_chunk[future])

    # Print summary
    console.print("\n[bold blue]Regridding Summary:[/bold blue]")
    console.print(f"[green]Successful:[/green] {success_count}")
    console.print(f"[red]Failed:[/red] {fail_count}")
    console.print(f"[white]Skipped:[/white] {skipped}")
    console.print(f"[blue]Total processed:[/blue] {success_count + fail_count}")

    # end timer
    end_time = time.localtime()
    console.print(f":clock3: END: {time.strftime('%Y-%m-%d %H:%M:%S', end_time)}\n")
    duration = time.mktime(end_time) - time.mktime(start_time)
    mins, secs = divmod(int(duration), 60)
    hours, mins = divmod(mins, 60)
    console.print(f":clock3: DURATION: {hours:02d}:{mins:02d}:{secs:02d}\n")


def _process_file_chunk_safe(
    file_chunk, fs, target_res, periodic, reuse_weights, delete_original
):
    """
    Process a chunk of files in a single process with proper ESMF environment setup.
    This function runs in its own process, so ESMF conflicts are avoided.
    """

    # Set up ESMF environment for this process
    os.environ["ESMF_MOAB"] = "0"
    os.environ["ESMF_NETCDF"] = "1"
    os.environ["ESMF_PIO"] = "0"
    os.environ["ESMF_LAPACK"] = "0"
    os.environ["ESMF_COMM"] = "mpiuni"
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["NUMBA_NUM_THREADS"] = "1"

    # Import here to ensure clean ESMF state
    import xarray as xa
    from reefshift.dataloading import processdata

    # Import the regrid module in this process
    from esgpulllite.custom.regrid import RegridderManager

    success_count = 0
    fail_count = 0

    console = Console()

    for ncfile, out_file in file_chunk:
        short_path = str(Path(*ncfile.parts[-6:]))

        try:
            # Force cleanup before each file
            gc.collect()

            # Load dataset
            ds = xa.open_dataset(ncfile)
            ds = ds.rename({"latitude": "lat", "longitude": "lon"})

            # Create regridder manager (this is where your working regrid_ds is used)
            regrid_mgr = RegridderManager(
                ds=ds, fs=fs, target_res=target_res, periodic=periodic
            )

            # Use your working regrid_ds method
            regridded_ds = regrid_mgr.regrid_ds(reuse_weights=reuse_weights)
            regridded_ds = processdata.process_xa_d(regridded_ds)

            # Save to output file
            regridded_ds.to_netcdf(out_file)

            # Determine regridding method for logging
            method_used = "CDO" if "ncells" in ds.dims else "xESMF"
            console.print(f"[green]Regridded ({method_used}):[/green] {short_path}")

            success_count += 1

            # Clean up original file if requested
            if delete_original:
                ncfile.unlink()

            # Force cleanup after each file
            del ds, regridded_ds, regrid_mgr
            gc.collect()

        except Exception as e:
            console.print(
                f"[red]Failed to regrid:[/red] {short_path} [red]{str(e)[:100]}...[/red]"
            )
            fail_count += 1
            # Force cleanup on error too
            gc.collect()

    return success_count, fail_count


def regrid_files_sequential_safe(
    watch_dir,
    subdir="reprojected",
    delete_original=False,
    fs=None,
    target_res=None,
    periodic=True,
    reuse_weights=True,
):
    """
    Sequential file processing that leverages your working regrid_ds function.
    This is the safest approach for ESMF - no parallelization at the file level,
    but uses your proven regrid_ds method.
    """
    watch_dir = Path(watch_dir)
    files_to_regrid = []
    skipped = 0

    # Find all NetCDF files that need regridding
    for ncfile in watch_dir.rglob("*.nc*"):
        if "reprojected" in str(ncfile) or "regrid_weights" in str(ncfile):
            continue
        parent_dir = ncfile.parent
        out_dir = parent_dir / subdir
        out_dir.mkdir(exist_ok=True)
        out_file = out_dir / ncfile.name
        if out_file.exists():
            skipped += 1
            continue
        files_to_regrid.append((ncfile, out_file))

    console = Console()
    success_count = 0
    fail_count = 0

    start_time = time.localtime()
    console.print(f":clock3: START: {time.strftime('%Y-%m-%d %H:%M:%S', start_time)}\n")

    if files_to_regrid:
        console.print(
            f"[blue]Processing {len(files_to_regrid)} files sequentially...[/blue]"
        )

        for ncfile, out_file in files_to_regrid:
            short_path = str(Path(*ncfile.parts[-6:]))

            try:
                # Load dataset
                ds = xa.open_dataset(ncfile)
                ds = ds.rename({"latitude": "lat", "longitude": "lon"})

                # Create regridder manager and use your working regrid_ds method
                regrid_mgr = RegridderManager(
                    ds=ds, fs=fs, target_res=target_res, periodic=periodic
                )

                # Use your proven regrid_ds method
                regridded_ds = regrid_mgr.regrid_ds(reuse_weights=reuse_weights)
                regridded_ds = processdata.process_xa_d(regridded_ds)

                # Save to output file
                regridded_ds.to_netcdf(out_file)

                # Determine regridding method for logging
                method_used = "CDO" if "ncells" in ds.dims else "xESMF"
                console.print(f"[green]Regridded ({method_used}):[/green] {short_path}")

                success_count += 1

                # Clean up original file if requested
                if delete_original:
                    ncfile.unlink()

            except Exception as e:
                console.print(
                    f"[red]Failed to regrid:[/red] {short_path} [red]{e}[/red]"
                )
                fail_count += 1

        # Print summary
        console.print("\n[bold blue]Regridding Summary:[/bold blue]")
        console.print(f"[green]Successful:[/green] {success_count}")
        console.print(f"[red]Failed:[/red] {fail_count}")
        console.print(f"[white]Skipped:[/white] {skipped}")
        console.print(f"[blue]Total processed:[/blue] {success_count + fail_count}")
    else:
        console.print("[yellow]No new files to regrid.[/yellow]")

    # end timer
    end_time = time.localtime()
    console.print(f":clock3: END: {time.strftime('%Y-%m-%d %H:%M:%S', end_time)}\n")
    duration = time.mktime(end_time) - time.mktime(start_time)
    mins, secs = divmod(int(duration), 60)
    hours, mins = divmod(mins, 60)
    console.print(f":clock3: DURATION: {hours:02d}:{mins:02d}:{secs:02d}\n")


def regrid_batch_safe(
    watch_dir, fs=None, target_res=(1, 1), max_workers=1, use_parallel=False
):
    """
    Simple wrapper for safe batch regridding that avoids ESMF rc=545 errors.

    This function chooses the safest approach based on your preferences.

    Parameters
    ----------
    watch_dir : str or Path
        Directory to scan for NetCDF files
    fs : optional
        File system object
    target_res : tuple
        Target resolution as (lon_res, lat_res)
    max_workers : int
        Number of workers (only used if use_parallel=True)
    use_parallel : bool
        Whether to use process-based parallelization (experimental)

    Returns
    -------
    None
        Files are regridded and saved to 'reprojected' subdirectories
    """
    if use_parallel and max_workers > 1:
        print(f"Using process-based parallelization with {max_workers} workers")
        regrid_files_safely_parallel(
            watch_dir=watch_dir,
            fs=fs,
            max_workers=max_workers,
            target_res=target_res,
            periodic=True,
            reuse_weights=True,
            chunk_size=1,
        )
    else:
        print("Using sequential processing (most reliable)")
        regrid_files_sequential_safe(
            watch_dir=watch_dir,
            fs=fs,
            target_res=target_res,
            periodic=True,
            reuse_weights=True,
        )


def main():
    """Command-line interface for regridding operations."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Regrid NetCDF files in a directory tree",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Required arguments
    parser.add_argument(
        "watch_dir",
        help="Directory to scan for NetCDF files",
    )

    # Optional arguments
    parser.add_argument(
        "--subdir",
        default="reprojected",
        help="Subdirectory name for regridded files",
    )

    parser.add_argument(
        "--target-res",
        nargs=2,
        type=float,
        default=[1.0, 1.0],
        metavar=("LON_RES", "LAT_RES"),
        help="Target resolution as longitude and latitude degrees",
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Maximum number of parallel workers",
    )

    parser.add_argument(
        "--delete-original",
        action="store_true",
        help="Delete original files after successful regridding",
    )

    parser.add_argument(
        "--no-periodic",
        action="store_true",
        help="Disable periodic boundary conditions",
    )

    parser.add_argument(
        "--no-reuse-weights",
        action="store_true",
        help="Don't reuse weight files (slower but more memory efficient)",
    )

    args = parser.parse_args()

    # Convert arguments
    target_res = tuple(args.target_res)
    periodic = not args.no_periodic
    reuse_weights = not args.no_reuse_weights

    # Run the regridding
    # regrid_all_files_in_tree(
    #     watch_dir=args.watch_dir,
    #     subdir=args.subdir,
    #     delete_original=args.delete_original,
    #     max_workers=args.max_workers,
    #     target_res=target_res,
    #     periodic=periodic,
    #     reuse_weights=reuse_weights,
    # )
    regrid_files_sequential_safe(
        watch_dir=args.watch_dir,
        # fs=fs,
        # target_res=(1, 1),
        target_res=target_res,  # Use None to avoid setting target_res
        periodic=periodic,
        reuse_weights=reuse_weights,
        # max_workers=1,
    )


if __name__ == "__main__":
    main()
