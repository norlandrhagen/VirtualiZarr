from __future__ import annotations

import io
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    import fsspec.core
    import fsspec.spec
    from cloudpathlib import AnyPath
    from fsspec.implementations.http import HTTPFileSystem

    # See pangeo_forge_recipes.storage
    OpenFileType = Union[
        fsspec.core.OpenFile, fsspec.spec.AbstractBufferedFile, io.IOBase
    ]


from dataclasses import dataclass
from urllib.parse import urlparse

from cloudpathlib import AnyPath, CloudPath


@dataclass
class PathType:
    pathtype: str  # should this be a cloudpathtype?
    cloud_prefix: Optional[str] = None


def _determine_path_type(filepath: str) -> PathType:
    """Utility to determine if input filepath is from a cloud provider, local or http(s)

    Parameters
    ----------
    filepath : str
        Input filepath

    Returns
    -------
    PathType
        class with filepath prefix information
    """

    # see if http/https url can be parsed
    parsed_url = urlparse(filepath)

    if parsed_url.scheme in ["http", "https"]:
        return PathType("http")

    path = AnyPath(filepath)

    # check if path is a cloudpath
    if isinstance(path, CloudPath):
        return PathType(pathtype="cloud", cloud_prefix=path.cloud_prefix)

    # TODO: better way to check if filepath is local?
    else:
        return PathType(pathtype="local")


def _cloudpathlib_transform(*, filepath: AnyPath | str) -> AnyPath | HTTPFileSystem:
    # TODO: What are all possible return types. S3Path, PosixPath etc.

    pathtype = _determine_path_type(filepath=filepath)
    cfilepath = _cloudpathlib_from_filepath(filepath=filepath)

    if pathtype.pathtype == "local":
        return cfilepath.as_posix()

    elif pathtype.pathtype == "cloud":
        return cfilepath.as_uri()

    elif pathtype.pathtype == "http":
        from fsspec.implementations.http import HTTPFileSystem

        fs = HTTPFileSystem()
        return fs.open(cfilepath)

    else:
        raise NotImplementedError(f"{pathtype.pathtype} is not local, http or cloud.")


def _cloudpathlib_from_filepath(
    *, filepath: str, reader_options: Optional[dict] = {}
) -> AnyPath | str:
    # cloudpathlib doesn't have an HTTP type, so for now, we can return str for http.
    from cloudpathlib import AnyPath

    pathtype = _determine_path_type(filepath=filepath)

    if pathtype.pathtype == "http":
        return filepath

    elif pathtype.pathtype == "local":
        return AnyPath(filepath)

    elif pathtype.pathtype == "cloud":
        return AnyPath(filepath)
    else:
        raise NotImplementedError(f"PathType: {pathtype.pathtype} is not supported")
