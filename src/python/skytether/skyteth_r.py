"""
A thin module that just imports functions and classes into the current namespace for use by R
scripts that use reticulate.
"""

# ------------------------------
# pylint directives

# pylint: disable=unused-import
# pylint: disable=wrong-import-order
# pylint: disable=invalid-name
# pylint: disable=no-name-in-module


# ------------------------------
# Dependencies

# >> Standard library classes
from dataclasses               import dataclass

# >> Standard library functions
from operator                  import attrgetter

# >> types and functions for R interface via reticulate

#   |> types
from pandas                    import DataFrame
from numpy                     import ndarray

#   |> functions
from numpy                     import array

# >> cython functions
from scytether                 import PyKineticConn
from scytether                 import ( UnionIndex
                                       ,UpdateIndex
                                       ,ExtendIndex
                                       ,DropDuplicates as DeduplicateGenes
                                       ,PrintDataStats
                                       ,PyExtendTable
                                      )

# >> functions
from skytether.writers         import WriteIPCFileFromTable
from skytether.dataformats     import EncodeStr

# wrapper functions, to be re-implemented later
from skytether.kctl            import KineticRange

# >> classes
from skytether.skyhook         import SkyhookPartition

# >> variables
#    |> system-based constants
from skytether.skyhook         import KINETIC_KV_MAXSIZE

#    |> data-based constants
from skytether.skyhook         import SLICE_SUFFIX, INDEX_SUFFIX


# ------------------------------
# Functions

def PartitionsFromDomain(domain_keyspace):
    """ Calls KineticRange, returning only partition names. """

    domain_keys    = KineticRange(
         key_start=f'{domain_keyspace}/'
        ,key_end  =f'{domain_keyspace}0'
    )

    def FnFilterPartitions(key_name):
        return (
                SLICE_SUFFIX not in key_name
            and INDEX_SUFFIX not in key_name
        )

    return list(filter(FnFilterPartitions, domain_keys))


def ScanPartition(domain_key, partition_key):
    """ Reads all RecordBatches for the given partition. """

    expr_partition = (
        SkyhookPartition.InDomain(domain_key)
                        .WithName(partition_key)
    )

    return expr_partition.ReadAll()


def ArrowCBind(left_table, right_table):
    """ An interface to PyExtendTable for R. """

    return PyExtendTable(left_table, right_table)
