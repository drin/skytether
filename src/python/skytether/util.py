"""
General utility functions that are likely to be used from a variety of contexts.
"""


# ------------------------------
# Dependencies

import os

# numpy is used for some compute-focused functions
import numpy


DEFAULT_NUMPY_TYPE = numpy.uint16


# ------------------------------
# convenience functions

def GetSliceIndex(key_name):
    """ Given a key, return its index value. """
    slice_ndx = os.path.splitext(key_name)[1]
    return (
        -1
        if   not slice_ndx
        else int(slice_ndx[1:])
    )


def NextKey(key_name):
    """ Returns the next key by incrementing the last byte of the given key. """

    encoded_key = key_name.encode('utf-8')
    next_encoded_key = (
          encoded_key[:-1]
        + (encoded_key[-1] + 1).to_bytes(1, byteorder='big')
    )

    return next_encoded_key.decode('utf-8')


def NormalizeStr(str_or_bytes, byte_encoding='utf-8'):
    """ Standard sanitizing function for strings. """
    if isinstance(str_or_bytes, bytes):
        return str_or_bytes.decode(byte_encoding)

    return str_or_bytes


def NormalizeRecord(record_as_str, delim='\t'):
    """ Common string sanitization. """
    return NormalizeStr(record_as_str).strip().strip('"').split(delim)


def BatchIndices(element_count, batch_size):
    """
    Generator that produces tuples of the format:
        (<batch ID>, <batch start>, <batch end>)

    batch ID - The "position" or "ordinal" of the produced batch, starting at 1.
    batch start - The first value (index) of the produced batch.
    batch end   - The final value (index) of the produced batch.

    The total range of values (indices) is from 0 to :element_count:. The total range is then split
    into many batches, where the number of values contained in each batch is equal to :batch_size:.
    """

    for batch_id, batch_start in enumerate(range(0, element_count, batch_size), start=1):
        batch_end = min(batch_id * batch_size, element_count)
        yield batch_id, batch_start, batch_end
