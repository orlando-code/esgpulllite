import importlib.metadata

try:
    # __package__ should be "esgpull" when this module is imported as part of the package.
    # Using "esgpull" directly if __package__ is None (e.g. script execution, though not typical here).
    dist_name = __package__ or "esgpull"
    __version__ = importlib.metadata.version(dist_name)
except importlib.metadata.PackageNotFoundError:
    # This fallback is useful in development environments where 'esgpull'
    # might be imported directly from the source tree without being fully
    # "installed" in a way that importlib.metadata can find its metadata.
    # This can occur if the current working directory or sys.path allows
    # Python to find the 'esgpull' directory before finding an installed version.
    # For a production build or after `pdm install`, the metadata should ideally be available.
    # If this fallback is frequently hit in a PDM-managed environment,
    # it might indicate an issue with the environment setup or how the
    # Jupyter kernel/Python interpreter is being launched.
    __version__ = "0.0.0.dev0+unknown"  # PEP 440 compliant placeholder
