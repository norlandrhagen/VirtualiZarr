import pytest
import xarray as xr

from virtualizarr.utils import (
    PathType,
    _determine_path_type,
)


@pytest.fixture
def dataset() -> xr.Dataset:
    return xr.Dataset(
        {"x": xr.DataArray([10, 20, 30], dims="a", coords={"a": [0, 1, 2]})}
    )


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
