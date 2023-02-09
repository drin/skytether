"""
Module that handles general skyhook API.
"""

# pylint: disable=wrong-import-order
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=invalid-name
# pylint: disable=multiple-statements
# pylint: disable=no-else-return

# useful for mocking and placeholders
# pylint: disable=unused-argument
# pylint: disable=too-few-public-methods
# pylint: disable=no-name-in-module

# ------------------------------
# Dependencies

import sys

# third-party
import pyarrow

# functions
from skytether.dataformats import EncodeInt64, EncodeInt8, DecodeInt

# variables
from skytether.dataformats import STRING_ENCODING

# cython functions
# NOTE: pylint can't find some names in scytether, so we disable the error above
from scytether import PyKineticConn, PyDomain, PyPartitionMeta, PyPartition


# ------------------------------
# Module Variables

# 1 MiB in bytes
KINETIC_KV_MAXSIZE = 1024 * 1024

# keyspace layout constants
SLICE_SUFFIX       = ';'
INDEX_SUFFIX       = '.'


# ------------------------------
# Classes

class SkyhookPartitionMeta:
    """
    Constructor requires a pyarrow.Schema instance
    """

    key_pname       = 'skyhook_pname'.encode(STRING_ENCODING)
    key_pcount      = 'skyhook_pcount'.encode(STRING_ENCODING)
    key_stripe_size = 'skyhook_stripe_size'.encode(STRING_ENCODING)

    def __init__(self, partition_schema, **kwargs):
        super().__init__(**kwargs)

        # make sure metadata is not None, for convenience
        if partition_schema.metadata is not None:
            self.sky_schema = partition_schema

        else:
            self.sky_schema = partition_schema.with_metadata({})

    def GetSchemaMetadata(self):
        return self.sky_schema.metadata

    def GetMetaKey(self, encoded_key, default_val=None):
        return self.sky_schema.metadata.get(encoded_key, default_val)

    def UpdateMetaKey(self, encoded_key, encoded_val):
        # we have to grab a reference to the metadata, then update it
        updated_meta              = self.sky_schema.metadata
        updated_meta[encoded_key] = encoded_val

        # then, we can create a new schema with updated metadata
        self.sky_schema = self.sky_schema.with_metadata(updated_meta)

        return self

    def GetName(self):
        return (
            self.GetMetaKey(self.__class__.key_pname, b'')
                .decode(STRING_ENCODING)
        )

    def SetName(self, new_name):
        return self.UpdateMetaKey(
             self.__class__.key_pname
            ,new_name.encode(STRING_ENCODING)
        )

    def GetPartitionCount(self):
        return DecodeInt(
            self.GetMetaKey(self.__class__.key_pcount, b'\x00')
        )

    def GetStripeSize(self):
        return DecodeInt(
            self.GetMetaKey(self.__class__.key_stripe_size, b'\x00')
        )

    def SetPartitionCount(self, new_count):
        encoded_count = EncodeInt64(new_count)
        # decoded_count = DecodeInt(encoded_count)

        print(f'Setting partition count to: {new_count}')
        # print(f'-> [{encoded_count}] -> [{decoded_count}]')

        return self.UpdateMetaKey(
             self.__class__.key_pcount
            ,encoded_count
        )

    def SetStripeSize(self, new_size):
        encoded_size = EncodeInt8(new_size)
        decoded_size = DecodeInt(encoded_size)

        print(f'Setting partition count to: {new_size}')
        print(f'-> [{encoded_size}] -> [{decoded_size}]')

        return self.UpdateMetaKey(
             self.__class__.key_stripe_size
            ,encoded_size
        )


class SkyhookPartitionSlice:
    def __init__(self, partition_meta, record_batch, **kwargs):
        super().__init__(**kwargs)

        # TODO: for now, we develop as if only partition metadata keeps metadata
        self.record_batch = record_batch.replace_schema_metadata({})

    def AsRecordBatch(self):
        return self.record_batch

    def AsTable(self):
        return pyarrow.Table.from_batches([self.record_batch])


class SkyhookPartition:

    @classmethod
    def InDomain(cls, domain_key):
        return cls(domain_key)

    def __init__(self, domain_name, **kwargs):
        super().__init__(**kwargs)

        self.domain = PyDomain.FromKey(domain_name)
        self.name   = ''
        self.meta   = None
        self.data   = None

    def WithName(self, pname):
        self.name = pname
        self.meta = PyPartitionMeta.FromKey(pname)

        return self

    # NOTE: this can easily be changed to slices later
    # def WithData(self, pslices):
    def WithData(self, partition_data):
        # TODO
        # self.data = PyPartition.

        return self
