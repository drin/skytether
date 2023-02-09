# ------------------------------
# Dependencies

# >> Cython dependencies
from libc.stdint   cimport int64_t, uint64_t
from libcpp        cimport bool
from libcpp.memory cimport unique_ptr, shared_ptr, make_shared
from libcpp.string cimport string
from libcpp.vector cimport vector

# >> pyarrow dependencies
#    > programmatic support
from pyarrow.lib cimport CStatus, CResult

#    > memory types
from pyarrow.lib cimport CBuffer

#    > tabular types
from pyarrow.lib cimport CSchema, CKeyValueMetadata, CTable, CRecordBatch, CChunkedArray


# ------------------------------
# C++ Definitions

# >> From skytether.hpp
cdef extern from '<skytether/skytether.hpp>':

    # Define static C++ functions
    cdef void PrintSchema(shared_ptr[CSchema] schema, int offset, int length)
    cdef void PrintTable (shared_ptr[CTable]  table , int offset, int length)


cdef extern from '<skytether/datatypes.hpp>':

    # Define a wrapper for a C++ class
    cdef cppclass KBuffer:

        # >> attributes
        char   *base
        size_t  len

        # >> constructors
        KBuffer() except +
        KBuffer(char *buf_base, size_t buf_len) except +

        # >> functions
        string ToString()

        void Release()


cdef extern from '<skytether/datamodel.hpp>':
    cdef cppclass Domain:
        string domain_key

        Domain(string) except +

        shared_ptr[Partition] PartitionFor(string partition_key)


    cdef cppclass PartitionMeta:

        # >> attributes
        int                           slice_width
        int                           slice_count
        string                        key
        shared_ptr[CSchema]           schema
        shared_ptr[CKeyValueMetadata] schema_meta

        # >> constructors
        PartitionMeta(string) except +

        # >> functions
        CStatus SetSchema(shared_ptr[CSchema] data_schema)


    cdef cppclass PartitionSlice:

        # >> attributes
        int                index
        string             key
        shared_ptr[CTable] data

        # >> constructors
        PartitionSlice(string, int) except +

        # >> functions
        int64_t num_rows()
        int     num_columns()
        CStatus SetData(shared_ptr[CTable] data_table)


    cdef cppclass Partition:

        # >> attributes
        Domain                             domain
        shared_ptr[PartitionMeta]          meta
        vector[shared_ptr[PartitionSlice]] slices

        # >> constructors
        Partition(Domain, shared_ptr[PartitionMeta]) except +

        # >> functions
        int64_t num_rows()
        int     num_columns()

        CResult[vector[shared_ptr[CTable]]] AsSlices()
        CResult[shared_ptr[CTable]]         AsTable()


cdef extern from '<skytether/operators.hpp>':

    # Define static C++ functions
    cdef CResult[shared_ptr[CBuffer]] WriteSchemaToBuffer(shared_ptr[CSchema] schema)

    cdef CResult[shared_ptr[CBuffer]] WriteRecordBatchToBuffer(
         shared_ptr[CSchema] schema
        ,shared_ptr[CRecordBatch] record_batch
    )

    cdef CResult[shared_ptr[CTable]] ExtendTable(
         shared_ptr[CTable] base_table
        ,shared_ptr[CTable] ext_table
    )

    # Wrapper for a C++ class
    cdef cppclass MeanAggr:
        # >> attributes
        uint64_t                  count
        shared_ptr[CChunkedArray] means
        shared_ptr[CChunkedArray] variances

        # >> constructors
        MeanAggr() except +

        # >> functions
        CStatus Initialize(shared_ptr[CChunkedArray] initial_vals)
        CStatus Accumulate(shared_ptr[CTable] value_batch, int64_t col_ndx)
        CStatus Combine(MeanAggr other_aggr)

        CStatus PrintState()
        CStatus PrintM1M2()

        shared_ptr[CTable] TakeResult()
        shared_ptr[CTable] ComputeTStatWith(MeanAggr other_aggr)


cdef extern from '<skytether/skykinetic.hpp>':

    #   |> for getting default connection params
    cdef string default_host
    cdef string default_port
    cdef string default_pass

    # Define a static C++ function
    cdef CResult[int] RowCountForByteSize(
         shared_ptr[CTable] table
        ,int                target_bytesize
        ,int                row_batchsize
    )


    cdef CResult[shared_ptr[CTable]] ProjectByName(
         shared_ptr[CTable] table_data
        ,vector[string]     attr_names
    )

    # Define a wrapper for a C++ class
    cdef cppclass KineticConn:

        # >> attributes
        int                conn_id
        shared_ptr[Domain] kinetic_domain

        #   |> for tracking data movement
        int keys_read
        int keys_written

        int bytes_read
        int bytes_written

        # >> constructors
        KineticConn()                   except +
        KineticConn(string domain_name) except +

        # >> functions
        bool IsConnected()

        void Disconnect()
        void Connect(string host, string port, string passwd) except +

        void PrintStats();

        # >> low-level functions
        CResult[unique_ptr[KBuffer]] GetKey(string key_name)
        CStatus                      PutKey(string key_name, shared_ptr[CBuffer] key_value)

        CResult[unique_ptr[KBuffer]] GetStripe(string key_name, int stripe_size)
        CStatus                      PutStripe(
             string              key_name
            ,shared_ptr[CBuffer] key_value
            ,int                 stripe_size
        )

        # >> high-level functions
        CResult[shared_ptr[PartitionMeta]]  GetPartitionMeta(string key_name)
        CResult[shared_ptr[Partition]]      GetPartitionData(string key_name)

        CStatus GetPartitionSlice(shared_ptr[PartitionSlice] pslice, int stripe_size)

        CResult[shared_ptr[CChunkedArray]] GetPartitionsByClusterNames(
            vector[string] cluster_names
        )

        CResult[shared_ptr[CSchema]] GetCellMetaByClusterNames(
             string         partition_key
            ,vector[string] cluster_names
        )

        CResult[shared_ptr[CTable]] GetCellsByClusterNames(
             string         partition_key
            ,vector[string] cluster_names
        )

        CStatus PutPartition(
             string             table_key
            ,shared_ptr[CTable] table_data
            ,int                byte_batch_size
            ,int                start_ndx
        )

        CStatus UnionIndex(
             string             index_keyspace
            ,shared_ptr[CTable] index_data
            ,int                byte_batch_size
        )

        CStatus UpdateIndex(
             string             index_keyspace
            ,shared_ptr[CTable] index_data
            ,int                byte_batch_size
        )

        CStatus ExtendIndex(
             string             index_keyspace
            ,shared_ptr[CTable] extension_data
            ,int                byte_batch_size
        )

        CStatus DropDuplicateRowsByKey(string partition_keyspace, int key_ndx)


# ------------------------------
# Extension type declarations

# cdef class PyDomain:
#     cdef shared_ptr[Domain] cpp_domain
# 
#     @staticmethod
#     cdef FromDomain(shared_ptr[Domain] domain)
# 
# 
# cdef class PyPartitionMeta:
#     cdef shared_ptr[PartitionMeta] cpp_pmeta
# 
#     @staticmethod
#     cdef PyPartitionMeta FromPartitionMeta(shared_ptr[PartitionMeta] pmeta)
# 
#     cdef CStatus SetSchema(self, shared_ptr[CSchema] meta_schema)
# 
# 
# cdef class PyPartitionSlice:
#     cdef shared_ptr[PartitionSlice] cpp_pslice
# 
#     @staticmethod
#     cdef PyPartitionSlice FromPartitionSlice(shared_ptr[PartitionSlice] pslice)
# 
#     cdef CStatus SetData(self, shared_ptr[CTable] slice_data)
# 
# 
# cdef class PyPartition:
#     cdef shared_ptr[Partition]     cpp_partition
#     cdef shared_ptr[PartitionMeta] cpp_pmeta
# 
#     @staticmethod
#     cdef PyPartition FromPartition(shared_ptr[Partition] partition)
# 
#     cdef CStatus SetData(self, shared_ptr[Partition] partition_data)

