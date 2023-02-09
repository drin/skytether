# ------------------------------
# Compiler directives

# distutils: language = c++


# ------------------------------
# Dependencies

# >> Modules
import sys
import cython


# >> Types
#   |> C   Cython includes
from libc.stdint   cimport int64_t
from libc.stdint   cimport int64_t, uint64_t

#   |> C++ Cython includes
from libcpp                    cimport nullptr, bool
from libcpp.memory             cimport unique_ptr, shared_ptr
from libcpp.string             cimport string
from libcpp.vector             cimport vector
from libcpp.utility            cimport move

#   |> pyarrow
from pyarrow.lib               cimport ( pyarrow_wrap_table
                                        ,pyarrow_unwrap_table
                                        ,pyarrow_wrap_schema
                                        ,pyarrow_unwrap_schema
                                        ,pyarrow_wrap_chunked_array
                                        ,pyarrow_unwrap_chunked_array
                                       )

from pyarrow.includes.common   cimport (CStatus, CResult)
from pyarrow.includes.libarrow cimport ( CSchema
                                        ,CTable
                                        ,CRecordBatch
                                        ,CChunkedArray
                                        ,CBuffer)

#   |> local
from skytether.cybindings.scytether cimport (KineticConn, MeanAggr, KBuffer)
from skytether.cybindings.scytether cimport ( Domain
                                             ,PartitionMeta
                                             ,PartitionSlice
                                             ,Partition)


# >> Functions
from cython import declare

from libcpp.memory             cimport make_shared

#   |> pyarrow
from pyarrow.includes.common   cimport (CStatus_OK, CStatus_Invalid, GetResultValue)

#   |> local
from skytether.cybindings.scytether cimport ( WriteSchemaToBuffer
                                             ,WriteRecordBatchToBuffer
                                             ,ExtendTable
                                             ,RowCountForByteSize
                                             ,PrintSchema
                                             ,PrintTable
                                             ,ProjectByName
                                            )


# >> Variables
from skytether.cybindings.scytether cimport (default_host, default_port, default_pass)
