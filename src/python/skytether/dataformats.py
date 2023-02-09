"""
Module that handles conversions between and handlers for various dataformats.
"""

import sys
import datetime

import numpy
import pyarrow


# ------------------------------
# Module-level reference variables

NUMERIC_ENDIAN  = 'big'
STRING_ENCODING = 'utf-8'


# ------------------------------
# Static data conversion functions

# >> for encoding KV metadata keys
def EncodeStr(key_name, encoding=STRING_ENCODING):
    """ Encodes a string value using :encoding: """
    return key_name.encode(encoding)


# >> for encoding of partition counts
def EncodeInt64(val, endian=NUMERIC_ENDIAN):
    """ Encodes a numeric value into 8 :endian: bytes """
    return val.to_bytes(8, byteorder=endian)


def EncodeInt8(val, endian=NUMERIC_ENDIAN):
    """ Encodes a numeric value into 1 :endian: bytes """
    return val.to_bytes(1, byteorder=endian)


def DecodeInt(val_as_bytes, endian=NUMERIC_ENDIAN):
    """ Decodes a numeric value into from :endian: bytes """
    return int.from_bytes(val_as_bytes, byteorder=endian)


# >> Data format conversions
def BinaryFromSchema(arrow_schema):
    """ Serializes an Arrow Schema, :arrow_schmea:, using IPC format. """

    arrow_buffer  = pyarrow.BufferOutputStream()
    stream_writer = pyarrow.RecordBatchStreamWriter(arrow_buffer, arrow_schema)

    stream_writer.close()
    return arrow_buffer.getvalue()


def BinaryFromTable(arrow_table):
    """ Serializes an Arrow Table, :arrow_table:, using IPC format. """
    arrow_buffer  = pyarrow.BufferOutputStream()
    stream_writer = pyarrow.RecordBatchStreamWriter(arrow_buffer, arrow_table.schema)

    for record_batch in arrow_table.to_batches():
        stream_writer.write_batch(record_batch)

    stream_writer.close()

    return arrow_buffer.getvalue()


def RecordBatchesFromBinary(data_blob):
    """ Opens an IPC stream from :data_blob: and returns a schema and list of RecordBatches. """
    stream_reader = pyarrow.ipc.open_stream(data_blob)

    # NOTE: assumes that stream_reader provides an iterator interface to reading each record batch
    return (
         stream_reader.schema
        ,(record_batch for record_batch in stream_reader)
    )


def TableFromBinary(data_blob):
    """ Opens an IPC stream from :data_blob: and returns an Arrow Table. """
    stream_reader = pyarrow.ipc.open_stream(data_blob)

    return stream_reader.read_all()


def SchemaFromBinary(data_blob):
    """ Opens an IPC stream from :data_blob: and returns an Arrow Table. """
    stream_reader = pyarrow.ipc.open_stream(data_blob)

    return stream_reader.schema


def NumpySliceFromDataFrame(input_df, row_ids, row_count, df_dtype, offset_rowndx=0):
    """
    Construct a horizontal slice of the input DataFrame using the given :row_count:. Returns the
    slice as a list of lists.
    """

    # we want the row_id as if we were doing a range
    batch_end   = min(offset_rowndx + row_count, input_df.shape[0])
    end_rowid   = row_ids[batch_end - 1]
    start_rowid = row_ids[offset_rowndx]

    column_arrays  = (
          [pyarrow.array(row_ids[offset_rowndx:batch_end])]
        + [
              pyarrow.array(
                  numpy.array(input_df.loc[start_rowid:end_rowid, col_id], dtype=df_dtype)
              )
              for col_id in input_df.columns
          ]
    )

    return column_arrays


def SlicesFromDataFrame(input_df, row_ids, row_count, df_dtype):
    """
    A generator that wraps `NumpySliceFromDataFrame` to generate all row slices of size :row_count:
    from the dataframe, :input_df:.
    """

    for batch_ndx, batch_row_start in enumerate(range(0, input_df.shape[0], row_count)):
        # NOTE: for debugging
        curr_time = datetime.datetime.now()
        sys.stdout.write(f'\r[{curr_time}] Generating slice from dataframe: [{batch_ndx}]')

        yield NumpySliceFromDataFrame(
             input_df
            ,row_ids
            ,row_count
            ,df_dtype
            ,batch_row_start
        )

    print('')
