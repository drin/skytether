"""
Module for code that writes data to files.
"""

# pylint: disable=unused-argument


# ------------------------------
# pylint directives

# pylint: disable=pointless-string-statement


# import logging
# import pyarrow

from skytether.dataformats import BinaryFromTable


def WriteIPCFromTable(arrow_table, media_handle):
    """ Write arrow table to file-like object, :media_handle:, in IPC format. """

    media_handle.write(BinaryFromTable(arrow_table))
    return True


def WriteIPCFileFromTable(arrow_table, table_filepath):
    """ Write arrow table to :table_filepath: in IPC format. """

    with open(table_filepath, 'wb') as output_filehandle:
        WriteIPCFromTable(arrow_table, output_filehandle)

    return table_filepath
