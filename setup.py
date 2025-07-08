from setuptools import find_packages, setup

setup(
    name="esgpulllite",
    version="0.1.0",
    description="My take on esgpull.",
    author="Orlando Timmerman",
    url="https://github.com/orlando-code/esgpulllite",
    packages=find_packages(),
    install_requires=[
        "matplotlib>=3.0",
        "numpy>=1.0,<2.3",  # Ensure compatibility with numba/xesmf
        "xarray",
        "xesmf",
        "cdo",
        "rich",
        "pathlib",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
