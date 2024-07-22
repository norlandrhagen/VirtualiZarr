from __future__ import annotations

import io
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    import fsspec.core
    import fsspec.spec
    from cloudpathlib import AnyPath

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


def _cloudpathlib_openfile_from_filepath(
    *, filepath: str, reader_options: Optional[dict] = {}
) -> AnyPath:
    from cloudpathlib import AnyPath
    # TODO: Should we raise an error if a filepath is passed to this util that is http?

    return AnyPath(filepath)


def _fsspec_openfile_from_filepath(
    *,
    filepath: str,
    reader_options: Optional[dict] = {},
) -> OpenFileType:
    """Converts input filepath to fsspec openfile object.

    Parameters
    ----------
    filepath : str
        Input filepath
    reader_options : _type_, optional
        Dict containing kwargs to pass to file opener, by default {'storage_options':{'key':'', 'secret':'', 'anon':True}}

    Returns
    -------
    OpenFileType
        An open file-like object, specific to the protocol supplied in filepath.

    Raises
    ------
    NotImplementedError
        Raises a Not Implemented Error if filepath protocol is not supported.
    """

    import fsspec
    from upath import UPath

    universal_filepath = UPath(filepath)
    protocol = universal_filepath.protocol

    if protocol == "s3":
        protocol_defaults = {"key": "", "secret": "", "anon": True}
    else:
        protocol_defaults = {}

    if reader_options is None:
        reader_options = {}

    storage_options = reader_options.get("storage_options", {})  # type: ignore

    # using dict merge operator to add in defaults if keys are not specified
    storage_options = protocol_defaults | storage_options
    fpath = fsspec.filesystem(protocol, **storage_options).open(filepath)

    return fpath
