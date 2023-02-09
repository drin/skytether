"""
Module that handles anything convenient but low level for skytether.
"""

# ------------------------------
# Compiler directives

# distutils: language = c++


# ------------------------------
# Dependencies

# >> Modules
import sys
import cython
import numpy
import pyarrow


# >> Types
#   |> C   Cython includes
from libc.stdint               cimport int64_t, uint64_t

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

from skytether.cybindings.scytether cimport MeanAggr, KBuffer, KineticConn
from skytether.cybindings.scytether cimport ( Domain
                                             ,PartitionMeta
                                             ,PartitionSlice
                                             ,Partition
                                             ,MeanAggr
                                            )

# from skytether.cybindings.scytether cimport ( PyDomain
#                                              ,PyPartitionMeta
#                                              ,PyPartitionSlice
#                                              ,PyPartition
#                                             )


# >> Functions
from cython import declare

from libcpp.memory             cimport make_shared

#   |> pyarrow
from pyarrow.includes.common   cimport (CStatus_OK, CStatus_Invalid, GetResultValue)

#   |> local
from skytether.cybindings.scytether cimport ( WriteSchemaToBuffer
                                             ,WriteRecordBatchToBuffer
                                             ,RowCountForByteSize
                                             ,ExtendTable
                                             ,ProjectByName
                                             ,PrintSchema
                                             ,PrintTable
                                            )


# ------------------------------
# Module-level reference variables

from skytether.cybindings.cykinetic cimport default_host, default_port, default_pass

NUMERIC_ENDIAN  = 'big'
STRING_ENCODING = 'utf-8'


# ------------------------------
# Functions

def SliceFromDataFrame(source_df, row_count, offset_rowndx=0):
    """ Extract a horizontal slice of the input DataFrame.  """

    # calculate the last row index
    slice_end   = min(offset_rowndx + row_count, source_df.shape[0])

    # grab start and stop row IDs to slice the dataframe
    rowid_stop  = source_df.index[slice_end - 1]
    rowid_start = source_df.index[offset_rowndx]

    column_arrays  = (
          [pyarrow.array(source_df.index[offset_rowndx:slice_end])]
        + [
              pyarrow.array(
                   source_df.loc[rowid_start:rowid_stop, col_id]
                  ,dtype=source_df.dtypes[col_ndx]
              )
              for col_ndx, col_id in enumerate(source_df.columns)
          ]
    )

    return column_arrays


def SlicesFromDataFrame(source_df, row_count):
    """ A generator (lazy) interface to `SliceFromDataFrame`.  """

    for batch_ndx, batch_row_start in enumerate(range(0, source_df.shape[0], row_count)):
        yield SliceFromDataFrame(source_df, row_count, batch_row_start)


def TablesFromPandas(self, py_schema, py_df, max_bytesize=1048576, sample_size=256):
    """ Constructs an Arrow Table from a Pandas DataFrame.  """

    # use a sample of the DataFrame to estimate optimal row count
    sample_slice = pyarrow.Table.from_arrays(
         SliceFromDataFrame(py_df, sample_size)
        ,schema=py_schema
    )

    # NOTE: This portion is fairly specific to kinetic
    # first, calculate row count without striping
    row_batchsize = FindOptimalRowCount(sample_slice, max_bytesize, sample_size)
    if not row_batchsize:
        # If row count is too high, try with striping (over 64 KVs)
        max_bytesize_stripe = 64 * max_bytesize
        row_batchsize       = FindOptimalRowCount(
            sample_slice, max_bytesize_stripe, sample_size
        )

    return (
        pyarrow.Table.from_arrays(table_slice, schema=py_schema)
        for table_slice in SlicesFromDataFrame(py_df, row_batchsize)
    )


# ------------------------------
# Cython extension classes

cdef class PyDomain:
    cdef shared_ptr[Domain] cpp_domain

    # >> Static and Class methods

    @staticmethod
    cdef FromDomain(shared_ptr[Domain] domain):
        domain_wrapper = PyDomain()
        domain_wrapper.cpp_domain = domain

    @staticmethod
    def FromKey(string domain_key):
        domain_wrapper = PyDomain()
        domain_wrapper.cpp_domain = make_shared[Domain](domain_key)


    # >> Instance methods

    def __cinit__(self, py_domainkey='Public'):
        cdef string cpp_domainkey = py_domainkey.encode('utf-8')
        self.cpp_domain           = make_shared[Domain](cpp_domainkey)

    def Key(self):
        return self.cpp_domain.get().domain_key

    def PartitionFor(self, py_pkey):
        cdef string                cpp_pkey      = py_pkey.encode('UTF-8')
        cdef shared_ptr[Partition] cpp_partition = self.cpp_domain.get().PartitionFor(cpp_pkey)

        return PyPartition.FromPartition(cpp_partition)


cdef class PyPartitionMeta:
    cdef shared_ptr[PartitionMeta] cpp_pmeta

    # >> Static and Class methods

    @staticmethod
    def FromKey(string partition_key):
        """ Construct a new instance using the name, :partition_key:. """

        meta_wrapper = PyPartitionMeta()
        meta_wrapper.cpp_pmeta = make_shared[PartitionMeta](partition_key)

        return meta_wrapper

    @staticmethod
    cdef FromPartitionMeta(shared_ptr[PartitionMeta] pmeta):
        """ Construct an instance that wraps :pmeta:. """

        meta_wrapper = PyPartitionMeta()
        meta_wrapper.cpp_pmeta = pmeta

        return meta_wrapper


    # >> Instance methods

    def __cinit__(self):
        self.cpp_pmeta = shared_ptr[PartitionMeta](nullptr)

    cdef CStatus SetSchema(self, shared_ptr[CSchema] meta_schema):
        """ Cpp function that takes a shared_ptr<pyarrow::Schema>. """

        return self.cpp_pmeta.get().SetSchema(meta_schema)

    def WithSchema(self, py_pschema):
        """ Python function that takes a pyarrow.Schema. """

        cdef shared_ptr[CSchema] cpp_pschema = pyarrow_unwrap_schema(py_pschema)
        cdef CStatus set_status = self.cpp_pmeta.get().SetSchema(cpp_pschema)

        if not set_status.ok(): return None
        return self

    def schema(self):
        return pyarrow_wrap_schema(self.cpp_pmeta.get().schema)

    def metadata(self):
        wrapped_schema = pyarrow_wrap_schema(self.cpp_pmeta.get().schema)
        if wrapped_schema is None: return None

        return wrapped_schema.metadata


cdef class PyPartitionSlice:
    cdef shared_ptr[PartitionSlice] cpp_pslice

    # >> Static and Class methods

    @staticmethod
    cdef FromPartitionSlice(shared_ptr[PartitionSlice] pslice):
        slice_wrapper = PyPartitionSlice()
        slice_wrapper.cpp_pslice = pslice

        return slice_wrapper

    @staticmethod
    def FromKey(string partition_key, int slice_ndx):
        slice_wrapper = PyPartitionSlice()
        slice_wrapper.cpp_pslice = make_shared[PartitionSlice](partition_key, slice_ndx)


    # >> Instance methods

    def __cinit__(self):
        self.cpp_pslice = shared_ptr[PartitionSlice](nullptr)

    cdef CStatus SetData(self, shared_ptr[CTable] slice_data):
        """ Cpp function that takes a shared_ptr<arrow::Table>. """

        return self.cpp_pslice.get().SetData(slice_data)

    def WithData(self, py_slicedata):
        """ Python function that takes a pyarrow.Table. """

        cdef shared_ptr[CTable] cpp_slicedata = pyarrow_unwrap_table(py_slicedata)
        cdef CStatus set_status = self.cpp_pslice.get().SetData(cpp_slicedata)

        if not set_status.ok(): return None
        return self

    def index(self):
        """ Returns the position of this slice within its partition. """

        return self.cpp_pslice.get().index

    def data(self):
        return pyarrow_wrap_table(self.cpp_pslice.get().data)


cdef class PyPartition:
    cdef Domain                    *cpp_domain
    cdef shared_ptr[Partition]      cpp_partition
    cdef shared_ptr[PartitionMeta]  cpp_pmeta

    @staticmethod
    cdef FromPartition(shared_ptr[Partition] partition):
        partition_wrapper = PyPartition()
        partition_wrapper.cpp_partition = partition
        partition_wrapper.cpp_pmeta     = partition.get().meta

        return partition_wrapper

    cdef CStatus SetData(self, shared_ptr[Partition] partition_data):
        """ Cpp function that takes a shared_ptr<Skytether::Partition>. """

        self.cpp_partition = partition_data
        return CStatus_OK()

    # @staticmethod
    # def FromPyPartition(py_pmeta, shared_ptr[Partition] partition):
    #     partition_wrapper = PyPartition()
    #     partition_wrapper.cpp_pmeta     = py_pmeta.cpp_pmeta
    #     partition_wrapper.cpp_partition = partition

    #     return partition_wrapper

    def __cinit__(self):
        self.cpp_partition = shared_ptr[Partition](nullptr)

    def __dealloc__(self):
        self.cpp_partition.reset()

    def chunk_size(self):
        return self.cpp_partition.get().num_rows()

    def num_columns(self):
        return self.cpp_partition.get().num_columns()

    def data(self):
        return pyarrow_wrap_table(GetResultValue(self.cpp_partition.get().AsTable()))

    def slices(self):
        cdef vector[shared_ptr[CTable]] pslices = GetResultValue(
            self.cpp_partition.get().AsSlices()
        )

        return [
            pyarrow_wrap_table(pslices[slice_ndx])
            for slice_ndx in range(pslices.size())
        ]

    def WithData(self, py_schema, py_df, slice_bytesize=1048576):
        cdef vector[shared_ptr[PartitionSlice]] pslices = self.cpp_partition.get().slices
        cdef CStatus set_status = CStatus_OK()

        py_schema        = pyarrow_wrap_schema(self.cpp_pmeta.get().schema)
        dataframe_slices = TablesFromPandas(py_schema, py_df, max_bytesize=slice_bytesize)
        for slice_ndx, py_slicedata in enumerate(dataframe_slices):
            set_status = pslices[slice_ndx].get().SetData(
                pyarrow_unwrap_table(py_slicedata)
            )

            if not set_status.ok(): return False

        return True

    def WriteAll(self, batch_size=0):
        """ Naive function to write expression to kinetic. """

        cdef string pkey = '/'.encode('utf-8') + self.cpp_pmeta.get().key

        # TODO: this needs to be refactored all the way down
        return self.kconn.WritePartition(pkey, self.data())

    def ReadCellClustersMeta(self, cluster_names):
        return self.kconn.ReadCellMetaByCluster(self.name, cluster_names)

    def ReadCellClusters(self, cluster_names):
        return self.kconn.ReadCellsByCluster(self.name, cluster_names)

    def ReadAll(self):
        partition_keyspace = self.name

        if self.metadata is not None and not self.name:
            partition_keyspace = self.metadata.GetName()

        return self.kconn.ReadPartition(partition_keyspace)


# This is a wrapper class for `MeanAggr`. Will coalesce the naming conventions when it
# feels right.
cdef class PyPartAggrStats:
    cdef MeanAggr paggr_cpp

    def __cinit__(self):
        self.paggr_cpp = MeanAggr()

    # NOTE: (ChunkedArray) -> Status
    def InitialVals(self, initial_tablecol):
        cdef shared_ptr[CChunkedArray] cpp_chunkedarr_ptr = (
            pyarrow_unwrap_chunked_array(initial_tablecol)
        )

        cdef CStatus init_status = self.paggr_cpp.Initialize(cpp_chunkedarr_ptr)

        if not init_status.ok(): return None
        return True

    # NOTE: (Table, int64_t) -> Status
    def MergeVals(self, new_table, col_startndx):
        cdef shared_ptr[CTable] cpp_tableptr = pyarrow_unwrap_table(new_table)

        cdef CStatus merge_status = self.paggr_cpp.Accumulate(cpp_tableptr, col_startndx)

        if not merge_status.ok(): return None
        return True


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


# ------------------------------
# Python Functions


def FindOptimalRowCount(py_table, target_bytesize, row_batchsize=20480):
    cdef shared_ptr[CTable] cpp_tableptr = pyarrow_unwrap_table(py_table)

    cdef CResult[int] row_count = RowCountForByteSize(cpp_tableptr, target_bytesize, row_batchsize)

    if not row_count.ok():
        sys.stderr.write('Could not determine optimal row count\n')
        return None

    return GetResultValue(row_count)


def PyExtendTable(py_basetable, py_exttable):
    cdef shared_ptr[CTable] cpp_basetable = pyarrow_unwrap_table(py_basetable)
    cdef shared_ptr[CTable] cpp_exttable  = pyarrow_unwrap_table(py_exttable)

    cdef CResult[shared_ptr[CTable]] ext_result = ExtendTable(cpp_basetable, cpp_exttable)
    if not ext_result.ok():
        return RuntimeError(ext_result.status().message().decode('UTF-8'))

    return pyarrow_wrap_table(GetResultValue(ext_result))


def DiffExprCellClusters(left_clustertable, right_clustertable):
    print('**Calling invalid version of differential expression**')

    cdef int64_t table_startcol = 1

    print('Calculating partial aggregates for left cluster')
    cdef MeanAggr left_stats  = MeanAggr()
    left_stats.Accumulate(
         pyarrow_unwrap_table(left_clustertable)
        ,table_startcol
    )

    print('Calculating partial aggregates for right cluster')
    cdef MeanAggr right_stats = MeanAggr()
    right_stats.Accumulate(
         pyarrow_unwrap_table(right_clustertable)
        ,table_startcol
    )

    print('Calculating t-statistic')
    cdef shared_ptr[CTable] tstat_results = left_stats.ComputeTStatWith(right_stats)

    return pyarrow_wrap_table(tstat_results)


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
