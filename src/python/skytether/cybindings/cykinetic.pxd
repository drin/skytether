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


# >> local dependencies
from skytether.cybindings.scytether cimport ( KBuffer
                                             ,Domain
                                             ,PartitionMeta
                                             ,PartitionSlice
                                             ,Partition
                                             ,MeanAggr
                                            )


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
