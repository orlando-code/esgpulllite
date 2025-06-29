# general
import numpy as np
import hashlib
from pathlib import Path
import tempfile
import re

# spatial
import xarray as xa
import xesmf as xe
from cdo import Cdo

# parallel
from concurrent.futures import ThreadPoolExecutor

# rich
from rich.console import Console


class RegridderManager:
    # TODO: getting a esmf warning when trying to regrid the same (deleted file): have to restart code
    def __init__(self, fs, ds, target_res=(1, 1), periodic=True):
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
        self.target_res = target_res
        self.varname = self._get_varname()
        self.ds = self._standardize_dims()
        self.ds = self._standardize_coords()
        self.ds = self._standardise_lon_limits()
        self.weight_dir = self.fs.data.parent / "xesmf_regrid_weights"
        self.weight_dir.mkdir(exist_ok=True)
        self.regridder = self._get_or_create_regridder()

    def get_target_resolution(self):
        # TODO
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
            if m := re.match(r"([\d.]+)km", res):
                if res == "10":
                    res = (0.1, 0.1)
                elif res == "25":
                    res = (0.25, 0.25)
                elif res == "50":
                    res = (0.5, 0.5)
                else:
                    res = float(res)
            if m := re.match(r"([\d.]+)x([\d.]+)degree", res):
                res = (float(m.group(1)), float(m.group(2)))
        # assume res is a tuple (lon_res, lat_res) and the same
        if isinstance(res, (int, float)):
            res = (res, res)

    def _infer_resolution_from_coords(self):
        """
        Infer resolution from the dataset's coordinates.
        Assumes coordinates are evenly spaced.
        Returns (1, 1) if unable to infer.
        """
        try:
            lon = (
                self.ds["lon"]
                if "lon" in self.ds.coords
                else self.ds["longitude"]
            )
            lat = (
                self.ds["lat"]
                if "lat" in self.ds.coords
                else self.ds["latitude"]
            )
            if (
                lon.ndim == 1
                and lat.ndim == 1
                and len(lon) > 1
                and len(lat) > 1
            ):
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
            return (lon_res, lat_res)
        except Exception:
            return (1, 1)

    def _get_varname(self, ncfile=None):
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
        return self.ds.rename_dims(dim_map)

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
        lon = (
            self.ds["lon"] if "lon" in self.ds.coords else self.ds["longitude"]
        )

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
        return xa.Dataset(
            {"lon": (["lon"], target_lon), "lat": (["lat"], target_lat)}
        )

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
                filename=weights_path,
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

    def regrid(self, time_index=None):
        data = self.ds[self.varname]
        if time_index is not None:
            data = data.isel(time=time_index)

        data = data.where(np.isfinite(data), drop=False)
        data.values[:] = np.ascontiguousarray(data.values)
        regridded_data = self.regridder(data)
        self.ds[self.varname] = regridded_data
        self.ds = self._trim_unnecessary_vals()
        return self.ds

    def regrid_with_cdo(
        self,
        ds: xa.Dataset,
        variable: str,
        grid_type: str = "remapcon",  # options: remapnn, remapbil, remapcon
        xsize: int = 360,
        ysize: int = 180,
        xfirst: float = -179.5,
        xinc: float = 1.0,
        yfirst: float = -89.5,
        yinc: float = 1.0,
    ) -> xa.Dataset:
        """
        Regrid an unstructured xarray.Dataset to a regular lat-lon grid using CDO.

        Parameters
        ----------
        ds : xa.Dataset
            Input dataset with unstructured grid (e.g., with lat_bnds/lon_bnds).
        variable : str
            Name of the variable to regrid.
        grid_type : str
            CDO regridding method: 'remapnn', 'remapbil', or 'remapcon'.
        xsize, ysize : int
            Output grid dimensions (longitude × latitude).
        xfirst, yfirst : float
            Starting coordinates for the grid.
        xinc, yinc : float
            Grid spacing in degrees.

        Returns
        -------
        xa.Dataset
            Regridded dataset with regular lat-lon grid.
        """
        cdo = Cdo()

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.nc"
            output_path = Path(tmpdir) / "output.nc"
            gridfile_path = Path(tmpdir) / "grid.txt"

            # Save dataset to disk
            ds[[variable]].to_netcdf(input_path)

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

            # Run CDO regridding
            getattr(cdo, grid_type)(
                gridfile_path, input=input_path, output=output_path
            )

            # Load result back into xarray
            ds_out = xa.open_dataset(output_path)

        return ds_out

    @staticmethod
    def regrid_all_files_in_tree(
        watch_dir,
        subdir="reprojected",
        delete_original=False,
        fs=None,
        max_workers=1,
    ):
        """
        Scan all subdirectories of watch_dir, create a 'reprojected' folder in each, and regrid all new .nc files
        in parallel. Only processes files that do not already have a regridded version.
        """

        watch_dir = Path(watch_dir)
        files_to_regrid = []
        for ncfile in watch_dir.rglob("*.nc"):
            if "reprojected" in str(ncfile):
                continue
            if "regrid_weights" in str(ncfile):
                continue
            parent_dir = ncfile.parent
            out_dir = parent_dir / subdir
            out_dir.mkdir(exist_ok=True)
            out_file = out_dir / ncfile.name
            if out_file.exists():
                continue
            files_to_regrid.append((ncfile, out_file))

        console = Console()

        def handle_file(args):
            ncfile, out_file = args
            try:
                ds = xa.open_dataset(ncfile)
                short_path = str(Path(*ncfile.parts[-6:]))

                if "ncells" in ds.dims:  # unstructured grid, use CDO
                    varname = RegridderManager._get_varname(ds)
                    if varname is None:
                        console.print(
                            f"[red]No suitable variable found in {ncfile}[/red]"
                        )
                        return
                    # get resolution
                    lat_res, lon_res = RegridderManager.get_target_resolution()
                    xsize = int(360 / lon_res)
                    ysize = int(180 / lat_res)
                    out_ds = RegridderManager.regrid_with_cdo(
                        ds, variable=varname, xsize=xsize, ysize=ysize
                    )
                    out_ds.to_netcdf(out_file)
                    console.print(
                        f"[green]Regridded with CDO:[/green] {short_path}"
                    )
                    if delete_original:
                        ncfile.unlink()

                else:

                    regrid_mgr = RegridderManager(fs=fs, ds=ds)
                    out_ds = ds.copy()
                    out_ds = regrid_mgr.regrid()
                    out_ds.to_netcdf(out_file)
                    console.print(f"[green]Regridded:[/green] {short_path}")
                    # disappear after 2 seconds
                    if delete_original:
                        ncfile.unlink()
            except Exception as e:
                console.print(
                    f"[red]Failed to regrid:[/red] {short_path} [red]{e}[/red]"
                )

        if files_to_regrid:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(handle_file, files_to_regrid)
            # TODO: Print summary panel at the end. Would require a refactor of the regrid_all_files_in_tree manager
            # to be more like the Download class
            # console.print(Panel(f"[green]Successful:[/green] {self.success_count}\n[red]Failed:[/red]
            # {self.fail_count}", title="Regridding Summary", style="bold blue"))
        else:
            console.print("[yellow]\nNo new files to regrid.[/yellow]")
