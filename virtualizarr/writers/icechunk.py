from typing import TYPE_CHECKING

import numpy as np
import zarr
from xarray import Dataset
from xarray.core.variable import Variable

from virtualizarr.manifests import ChunkManifest, ManifestArray

if TYPE_CHECKING:
    from icechunk import IcechunkStore


def dataset_to_icechunk(ds: Dataset, store: "IcechunkStore") -> None:
    """
    Write an xarray dataset whose variables wrap ManifestArrays to an Icechunk store.

    Currently requires all variables to be backed by ManifestArray objects.

    Parameters
    ----------
    ds: xr.Dataset
    store: IcechunkStore
    """
    from icechunk import IcechunkStore

    if not isinstance(store, IcechunkStore):
        raise TypeError(f"expected type IcechunkStore, but got type {type(store)}")

    # TODO only supports writing to the root group currently
    root_group = zarr.group(store=store, overwrite=True)

    # TODO this is Frozen, the API for setting attributes must be something else
    # root_group.attrs = ds.attrs

    for name, var in ds.variables.items():
        write_variable_to_icechunk(
            store=store,
            group=root_group,
            name=name,
            var=var,
        )

    return None


def write_variable_to_icechunk(
    store: "IcechunkStore",
    group: zarr.Group,
    name: str,
    var: Variable,
) -> None:
    if not isinstance(var.data, ManifestArray):
        # TODO is writing loadable_variables just normal xarray ds.to_zarr?
        raise NotImplementedError()

    ma = var.data
    zarray = ma.zarray

    # TODO when I try to create this array I get an AssertionError from inside Zarr v3
    # TODO do I need this array object for anything after ensuring the array has been created?
    # array = group.create_array(
    #     store,
    #     shape=zarray.shape,
    #     chunk_shape=zarray.chunks,
    #     dtype=zarray.dtype,
    # )

    # TODO we also need to set zarr attributes, including DIMENSION_NAMES

    write_manifest_virtual_refs(
        store=store,
        group=group,
        name=name,
        manifest=ma.manifest,
    )


def write_manifest_virtual_refs(
    store: "IcechunkStore",
    group: zarr.Group,
    name: str,
    manifest: ChunkManifest,
) -> None:
    """Write all the virtual references for one array manifest at once."""

    # loop over every reference in the ChunkManifest for that array
    # TODO inefficient: this should be replaced with something that sets all (new) references for the array at once
    # but Icechunk need to expose a suitable API first
    it = np.nditer(
        [manifest._paths, manifest._offsets, manifest._lengths],
        flags=[
            "refs_ok",
            "multi_index",
            "c_index",
        ],  # TODO is "c_index" correct? what's the convention for zarr chunk keys?
        op_flags=[["readonly"]] * 3,
    )
    for path, offset, length in it:
        index = it.multi_index
        chunk_key = "/".join(str(i) for i in index)

        # TODO again this async stuff should be handled at the rust level, not here
        # set each reference individually
        store.set_virtual_ref(
            # TODO it would be marginally neater if I could pass the group and name as separate args
            key=f"{group}/{name}/{chunk_key}",  # should be of form '/group/name/0/1/2'
            location=path.item(),
            offset=offset.item(),
            length=length.item(),
        )
