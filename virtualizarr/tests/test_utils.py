import contextlib
import pathlib

import fsspec
import fsspec.implementations.local
import fsspec.implementations.memory
import pytest
import xarray as xr

from virtualizarr.utils import (
    PathType,
    _determine_path_type,
    _fsspec_openfile_from_filepath,
)


@pytest.fixture
def dataset() -> xr.Dataset:
    return xr.Dataset(
        {"x": xr.DataArray([10, 20, 30], dims="a", coords={"a": [0, 1, 2]})}
    )


def test_fsspec_openfile_from_path(tmp_path: pathlib.Path, dataset: xr.Dataset) -> None:
    f = tmp_path / "dataset.nc"
    dataset.to_netcdf(f)

    result = _fsspec_openfile_from_filepath(filepath=f.as_posix())
    assert isinstance(result, fsspec.implementations.local.LocalFileOpener)


def test_determine_path_type():
    """Check if _determine_path_type can guess inputh filepath types"""

    assert PathType(pathtype="cloud", cloud_prefix="s3://") == _determine_path_type(
        "s3://virtualizarr/air.nc"
    )
    assert PathType(pathtype="cloud", cloud_prefix="gs://") == _determine_path_type(
        "gs://virtualizarr/air.nc"
    )
    # TODO: cloudpathlib.exceptions.MissingCredentialsError: AzureBlobClient does not support anonymous instantiation. Credentials are required; see docs for options.
    # assert PathType(pathtype='cloud', cloud_prefix='az://')  == _determine_path_type("az://virtualizarr/air.nc")
    assert PathType(pathtype="http") == _determine_path_type(
        "https://virtualizarr.com/air.nc"
    )
    assert PathType("local") == _determine_path_type("/home/virtualizarr/air.nc")


def test_cloudpathlib_openfile_from_filepath(tmp_path):
    pass


def test_fsspec_openfile_memory(dataset: xr.Dataset):
    fs = fsspec.filesystem("memory")
    with contextlib.redirect_stderr(None):
        # Suppress "Exception ignored in: <function netcdf_file.close at ...>"
        with fs.open("dataset.nc", mode="wb") as f:
            dataset.to_netcdf(f, engine="h5netcdf")

    result = _fsspec_openfile_from_filepath(filepath="memory://dataset.nc")
    with result:
        assert isinstance(result, fsspec.implementations.memory.MemoryFile)
