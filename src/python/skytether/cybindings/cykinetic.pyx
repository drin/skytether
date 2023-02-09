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
from libc.stdint    cimport int64_t
from libc.stdint    cimport int64_t, uint64_t

#   |> C++ Cython includes
from libcpp         cimport nullptr, bool
from libcpp.memory  cimport unique_ptr, shared_ptr
from libcpp.string  cimport string
from libcpp.vector  cimport vector
from libcpp.utility cimport move

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
from skytether.cybindings.scytether cimport MeanAggr, KBuffer
from skytether.cybindings.scytether cimport ( PyDomain
                                             ,PyPartitionMeta
                                             ,PyPartitionSlice
                                             ,PyPartition
                                            )
from skytether.cybindings.scytether  import ( PyDomain
                                             ,PyPartitionMeta
                                             ,PyPartitionSlice
                                             ,PyPartition
                                            )


# >> Functions
from cython                     import declare
from libcpp.memory             cimport make_shared

#   |> pyarrow
from pyarrow.includes.common   cimport (CStatus_OK, CStatus_Invalid, GetResultValue)

#   |> local
from skytether.cybindings.scytether cimport ( WriteSchemaToBuffer
                                             ,WriteRecordBatchToBuffer
                                             ,ExtendTable
                                             ,PrintSchema
                                             ,PrintTable
                                            )


# ------------------------------
# Cython extension classes

# A wrapper around KineticConn to make some of the cython code more readable
cdef class PyKineticConn:
    cdef KineticConn kconn_cpp

    # NOTE: less efficient, but makes the logic simpler for now
    default_khost = default_host.decode('utf-8')
    default_kport = default_port.decode('utf-8')
    default_kpass = default_pass.decode('utf-8')

    def __cinit__(self, domain_key=None):
        if domain_key is None:
            self.kconn_cpp = KineticConn()

        else:
            self.kconn_cpp = KineticConn(domain_key.encode('utf-8'))

    def __dealloc__(self):
        sys.stdout.write('Deallocating PyKineticConn...\n')
        self.kconn_cpp.Disconnect()
        sys.stdout.write('Deallocated!\n')

    # >> Core functions
    def BytesRead(self):
        return self.kconn_cpp.bytes_read

    def BytesWritten(self):
        return self.kconn_cpp.bytes_written

    def KeysRead(self):
        return self.kconn_cpp.keys_read

    def KeysWritten(self):
        return self.kconn_cpp.keys_written

    def Connect(self, host=default_khost, port=default_kport, passwd=default_kpass):
        self.kconn_cpp.Connect(
             host.encode('utf-8')
            ,port.encode('utf-8')
            ,passwd.encode('utf-8')
        )

    def IsConnected(self):
        return self.kconn_cpp.IsConnected()

    def PrintDataStats(self):
        self.kconn_cpp.PrintStats()

    # >> Read functions
    def ReadKeyValue(self, py_keyname):
        """ Wrapper to read a single kinetic KeyValue. """

        cdef string              cpp_keyname   = py_keyname.encode('UTF-8')
        cdef unique_ptr[KBuffer] cpp_valbuffer = move(
            GetResultValue(self.kconn_cpp.GetKey(cpp_keyname))
        )

        return cpp_valbuffer.get().ToString().decode('UTF-8')

    def ReadStripe(self, py_keyname, stripe_size=1):
        """ Wrapper to read data that is striped over many kinetic KeyValues (default: 1 KeyValue). """

        cdef string             cpp_key = py_keyname.encode('UTF-8')
        cdef unique_ptr[KBuffer] cpp_valbuffer = move(
            GetResultValue(self.kconn_cpp.GetStripe(cpp_key, stripe_size))
        )

        return cpp_valbuffer.get().ToString().decode('UTF-8')

    def ReadPartitionMeta(self, py_keyname):
        """ Wrapper to read just the metadata KeyValue for the partition, :py_keyname:. """

        cdef string                    cpp_key = py_keyname.encode('UTF-8')
        cdef shared_ptr[PartitionMeta] pmeta   = GetResultValue(
            self.kconn_cpp.GetPartitionMeta(cpp_key)
        )

        return pyarrow_wrap_schema(pmeta.get().schema)

    def ReadPartitionSlice(self, py_key, slice_ndx, stripe_size=1):
        """ Wrapper to read a single slice (e.g. RecordBatch or KeyValue) of a partition. """

        cdef string                     cpp_key = py_key.encode('UTF-8')
        cdef uint64_t                   cpp_ndx = slice_ndx
        cdef shared_ptr[PartitionSlice] pslice  = make_shared[PartitionSlice](cpp_key, cpp_ndx)

        cdef CStatus get_status = self.kconn_cpp.GetPartitionSlice(pslice, stripe_size)
        if not get_status.ok(): return None

        return pyarrow_wrap_table(pslice.get().data)

    def ReadPartition(self, py_pkey):
        """ Wrapper to get all data for the given partition, :py_pkey:. """

        cdef string cpp_pkey = py_pkey.encode('UTF-8')
        cdef CResult[shared_ptr[Partition]] partition_result = (
            self.kconn_cpp.GetPartitionData(cpp_pkey)
        )

        if not partition_result.ok(): return None

        return PyPartition.FromPartition(GetResultValue(partition_result))

    def ReadPartitionSlices(self, py_pmeta):
        """
        Wrapper to get all data for the `PyPartition` described by the given `PyPartitionMeta`,
        :py_pmeta:.
        """

        # cpp_pmeta accesses the shared_ptr[PartitionMeta] that py_pmeta wraps
        cdef CResult[shared_ptr[Partition]] partition_result = (
            self.kconn_cpp.GetPartitionData(py_pmeta.cpp_pmeta)
        )

        if not partition_result.ok(): return None

        return PyPartition.FromPartition(GetResultValue(partition_result))

    def ReadPartitionsByCluster(self, py_cnames):
        """ Wrapper to read partition keys, given a list of target cluster names. """

        cdef vector[string] cpp_cnames = py_cnames
        cdef CResult[shared_ptr[CChunkedArray]] arr_result = (
            self.kconn_cpp.GetPartitionsByClusterNames(cpp_cnames)
        )

        return pyarrow_wrap_chunked_array(GetResultValue(arr_result))

    def ReadCellMetaByCluster(self, py_partitionkey, py_cnames):
        """ Wrapper to read metadata for cells, given a list of target cluster names. """

        cdef string cpp_partitionkey   = py_partitionkey.encode('UTF-8')
        cdef vector[string] cpp_cnames = py_cnames

        cdef CResult[shared_ptr[CSchema]] schema_result = self.kconn_cpp.GetCellMetaByClusterNames(
            cpp_partitionkey, cpp_cnames
        )

        return pyarrow_wrap_schema(GetResultValue(schema_result))

    def ReadCellsByCluster(self, py_partitionkey, py_cnames):
        """ Wrapper to read cell-level gene expression, given a list of target cluster names. """

        cdef string cpp_partitionkey   = py_partitionkey.encode('UTF-8')
        cdef vector[string] cpp_cnames = py_cnames

        cdef CResult[shared_ptr[CTable]] table_result = self.kconn_cpp.GetCellsByClusterNames(
            cpp_partitionkey, cpp_cnames
        )

        return pyarrow_wrap_table(GetResultValue(table_result))


    # >> Write functions

    def WriteMetadata(self, py_keyname, py_schema):
        """ Wrapper to write a metadata KeyValue for a skyhook partition. """

        # serialize the schema
        cdef shared_ptr[CSchema]          cpp_schema   = pyarrow_unwrap_schema(py_schema)
        cdef CResult[shared_ptr[CBuffer]] write_result = WriteSchemaToBuffer(cpp_schema)
        if not write_result.ok():
            return RuntimeError(write_result.status().message().decode('UTF-8'))

        # write the key value
        cdef string              cpp_keyname   = py_keyname.encode('UTF-8')
        cdef shared_ptr[CBuffer] cpp_valbuffer = GetResultValue(write_result)
        cdef CStatus             put_status    = self.kconn_cpp.PutKey(cpp_keyname, cpp_valbuffer)

        if not put_status.ok():
            return RuntimeError(put_status.message().decode('UTF-8'))

        return None


    def WritePartition(self, py_keyname_prefix, py_table, byte_batch, start_ndx=0):
        """ Wrapper to write to a skyhook partition (metadata and data slices). """

        # prep the key string and arrow schema
        cdef string             cpp_keyprefix = py_keyname_prefix.encode('UTF-8')
        cdef shared_ptr[CTable] cpp_tableptr  = pyarrow_unwrap_table(py_table)

        # let the C++ core write the whole partition
        cdef CStatus put_status = self.kconn_cpp.PutPartition(
            cpp_keyprefix, cpp_tableptr, byte_batch, start_ndx
        )

        # check for error
        if not put_status.ok(): return RuntimeError(put_status.message().decode('UTF-8'))

        return None


    # >> High-level functions

    def UnionIndex(self, py_keyname, py_indexdata, byte_batch):
        # prep the key string and arrow schema
        cdef string              cpp_key       = py_keyname.encode('UTF-8')
        cdef shared_ptr[CTable]  cpp_tableptr  = pyarrow_unwrap_table(py_indexdata)

        # let the C++ core write the whole partition
        cdef CStatus update_status = self.kconn_cpp.UnionIndex(cpp_key, cpp_tableptr, byte_batch)

        # check for error
        if not update_status.ok():
            return RuntimeError(update_status.message().decode('UTF-8'))

        return None


    def UpdateIndex(self, py_keyname, py_indexdata, byte_batch):
        # prep the key string and arrow schema
        cdef string              cpp_key       = py_keyname.encode('UTF-8')
        cdef shared_ptr[CTable]  cpp_tableptr  = pyarrow_unwrap_table(py_indexdata)

        print(f'Updating index [{cpp_key}]')

        # let the C++ core write the whole partition
        cdef CStatus update_status = self.kconn_cpp.UpdateIndex(cpp_key, cpp_tableptr, byte_batch)

        # check for error
        if not update_status.ok():
            err_msg = update_status.message().decode('utf-8')
            sys.stderr.write(err_msg + '\n')
            return RuntimeError(err_msg)

        return None


    def ExtendIndex(self, py_keyname, py_indexdata, byte_batch):
        # prep the key string and arrow schema
        cdef string              cpp_key       = py_keyname.encode('UTF-8')
        cdef shared_ptr[CTable]  cpp_tableptr  = pyarrow_unwrap_table(py_indexdata)

        print(f'Extending index [{cpp_key}]')

        # let the C++ core write the whole partition
        cdef CStatus update_status = self.kconn_cpp.ExtendIndex(cpp_key, cpp_tableptr, byte_batch)

        # check for error
        if not update_status.ok():
            err_msg = update_status.message().decode('utf-8')
            sys.stderr.write(err_msg + '\n')
            return RuntimeError(err_msg)

        return None

    def DropDuplicateRowsByKey(self, py_dkey, py_pkey, pkey_ndx=0):
        cdef string cpp_partitionkey = (f'{py_dkey}/{py_pkey}').encode('UTF-8')

        cdef CStatus drop_result = (
            self.kconn_cpp.DropDuplicateRowsByKey(cpp_partitionkey, pkey_ndx)
        )

        if not drop_result.ok():
            return RuntimeError(drop_result.message().decode('UTF-8'))

        return True

    def DiffExprCentroids(self, py_dkey, py_leftcols, py_rightcols):
        cdef string  centroid_key   = (f'{py_dkey}.centroids').encode('utf-8')
        cdef int64_t table_startcol = 1

        print('Retrieving centroid data')
        cdef shared_ptr[Partition] centroid_partition = GetResultValue(
            self.kconn_cpp.GetPartitionData(centroid_key)
        )
        cdef shared_ptr[CTable] centroids = GetResultValue(
            centroid_partition.get().AsTable()
        )

        print('Calculating partial aggregates for left centroids')
        cdef vector[string] cpp_leftcols = py_leftcols
        cdef MeanAggr       left_stats   = MeanAggr()
        left_stats.Accumulate(
             GetResultValue(ProjectByName(centroids, cpp_leftcols))
            ,table_startcol
        )

        print('Calculating partial aggregates for right centroids')
        cdef vector[string] cpp_rightcols = py_rightcols
        cdef MeanAggr       right_stats   = MeanAggr()
        right_stats.Accumulate(
             GetResultValue(ProjectByName(centroids, cpp_rightcols))
            ,table_startcol
        )

        print('Calculating t-statistic')
        cdef shared_ptr[CTable] tstat_results = left_stats.ComputeTStatWith(right_stats)

        return pyarrow_wrap_table(tstat_results)
