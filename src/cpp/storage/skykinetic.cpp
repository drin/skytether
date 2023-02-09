/**
 * Copyright 2023 Aldrin Montana

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * An implementation of a kinetic connector.
 */

// ------------------------------
// Dependencies

#include "../headers/skykinetic.hpp"
#include "../headers/operators.hpp"

// this is for short-term benchmarking
#include <ctime>
#include <fstream>


// ------------------------------
// Macros and aliases

// variables
using Skytether::DOMAIN_DELIM;
using Skytether::META_DELIM;
using Skytether::SLICE_DELIM;

// functions
using Skytether::SetPartitionCount;
using Skytether::SetStripeSize;

using Skytether::GetPartitionCount;
using Skytether::GetStripeSize;


// ------------------------------
// Constants

// KineticConn default connection parameters
const string default_host { "10.23.89.4" };
const string default_port { "8123"       };
const string default_pass { "asdfasdf"   };


// 1 MiB
const int64_t  MAX_KV_SIZE         = 1048576;
const uint8_t  MAX_KV_STRIPE_SIZE  = 255;

// TODO: this is arbitrary and should be evaluated
const int64_t  MIN_ROWBATCH_SIZE   = 64;
const uint8_t  DEFAULT_STRIPE_SIZE = 64;

const int      MAX_ROW_CALCS       = 7;


// ------------------------------
// Functions and Methods

// hack-y
void
WriteData(const string &filename, size_t byte_count) {
    std::time_t   curr_time { std::time(nullptr)                      };
    std::ofstream output_fd { filename, std::ios::app | std::ios::out };

    output_fd << curr_time << "\t" << byte_count << std::endl;
    output_fd.close();
}


// >> Static functions

Result<int64_t>
BufferSizeForNextBatch(arrow::TableBatchReader &batch_reader, shared_ptr<Schema> table_schema) {
    shared_ptr<RecordBatch> curr_recordbatch;

    // zero-copy construct a RecordBatch, and determine it's serialization size
    ARROW_RETURN_NOT_OK(batch_reader.ReadNext(&curr_recordbatch));
    if (curr_recordbatch == nullptr) {
        return Status::UnknownError("BufferSizeForNextBatch: Failed to read batch");
    }

    auto serialize_result = WriteRecordBatchToBuffer(table_schema, curr_recordbatch);
    if (not serialize_result.ok()) {
        sky_debug_printf("Unable to serialize row data to determine row count\n");
        return Status::Invalid("BufferSizeForNextBatch: Failed to calculate size");
    }

    return serialize_result.ValueOrDie()->size();
}


// TODO: eventually we will want to be able to handle an initial row_batchsize that is
//       significantly under target
Result<int64_t>
RowCountForByteSize(shared_ptr<Table> table_data, int64_t target_bytesize, int64_t row_batchsize) {
    // actual byte size of recordbatch largest recordbatch (requires the smallest row count)
    int64_t min_bytesize = 0;
    int64_t min_rowcount = 0;

    // margin of error between serialized example and remaining batches?
    target_bytesize -= 500;

    // row_batchsize defaults to 20480, but should converge quickly over a few iterations
    for (int iter_count = 0; iter_count < MAX_ROW_CALCS; ++iter_count) {
        arrow::TableBatchReader batch_reader(*table_data);
        batch_reader.set_chunksize(row_batchsize);

        ARROW_ASSIGN_OR_RAISE(
             min_bytesize
            ,BufferSizeForNextBatch(batch_reader, table_data->schema())
        );

        // if we're over the target, update row count prediction and try again
        if (min_bytesize > target_bytesize) {
            row_batchsize = (int64_t) (target_bytesize * row_batchsize / min_bytesize);
            continue;
        }

        // we use a temporary variable because we need row_batchsize to correctly reflect the
        // chunksize of the TableBatchReader
        min_rowcount = (int64_t) (target_bytesize * row_batchsize / min_bytesize);

        // check several recordbatch samples and set row_batchsize to smallest calculated row count
        // NOTE: we only consider reducing row_batchsize to accommodate the largest recordbatch
        for (int sample_ndx = 1; sample_ndx < MAX_ROW_CALCS; ++sample_ndx) {
            auto size_result = BufferSizeForNextBatch(batch_reader, table_data->schema());
            if (not size_result.ok()) { break; }

            int64_t sample_bytesize = size_result.ValueOrDie();
            int64_t sample_rowcount = (int64_t) (target_bytesize * row_batchsize / sample_bytesize);
            if (sample_rowcount < min_rowcount) {
                min_rowcount = sample_rowcount;
                min_bytesize = sample_bytesize;
            }
        }
    }

    sky_debug_printf(
         "Selecting row count [%ld <= %ld bytes]: %ld\n"
        ,min_bytesize
        ,target_bytesize
        ,min_rowcount
    );

    return min_rowcount;
}


// >> KineticConn
KineticConn::~KineticConn() {
    kinetic_domain.reset();

    if (IsConnected()) { Disconnect(); }
}

KineticConn::KineticConn()  {
    conn_id        = -1;
    kinetic_domain = std::make_shared<Domain>("public");
}

KineticConn::KineticConn(const std::string &domain_name) {
    conn_id        = -1;
    kinetic_domain = std::make_shared<Domain>(domain_name);
}

bool
KineticConn::IsConnected() { return conn_id != -1; }

void
KineticConn::Disconnect() {
    bkv_close(conn_id);
    conn_id = -1;
}

void
KineticConn::Connect(const string &host, const string &port, const string &passwd) {
    bkvs_open_t open_options;

    open_options.bkvo_host   = (char *) host.data();
    open_options.bkvo_port   = (char *) port.data();
    open_options.bkvo_pass   = (char *) passwd.data();

    open_options.bkvo_id     = 1;
    open_options.bkvo_usetls = 0;

    conn_id = bkv_open(&open_options);
}

void
KineticConn::PrintStats() {
    std::cout << "Connection status (id: " << conn_id << ")" << std::endl;
    double mib_amount;

    mib_amount = ((double) bytes_read) / MAX_KV_SIZE;
    std::cout << "Gets: "
              << std::endl
              << "\tbytes    -> " << bytes_read << " (" << mib_amount << " MiB)"
              << std::endl
              << "\trequests -> " << keys_read
              << std::endl
    ;

    mib_amount = ((double) bytes_written) / MAX_KV_SIZE;
    std::cout << "Puts: "
              << std::endl
              << "\tbytes    -> " << bytes_written << " (" << mib_amount << " MiB)"
              << std::endl
              << "\trequests -> " << keys_written
              << std::endl
    ;

    auto    mempool     = arrow::default_memory_pool();
    int64_t bytes_alloc = mempool->bytes_allocated();
    mempool->ReleaseUnused();

    std::cout << "Arrow Memory: "
              << std::endl
              << "\tbytes (pre-release)  -> " << bytes_alloc
              << std::endl
              << "\tbytes (post-release) -> " << mempool->bytes_allocated()
              << std::endl
    ;
}


Status
KineticConn::PutKey(const string& key_name, shared_ptr<Buffer> key_val) {
    // string canonical_key { kinetic_domain->domain_key + key_name };
    string canonical_key { key_name };

    int put_status = bkv_put(
         conn_id                         // conn data
        ,(void *) canonical_key.data()   //  key data
        ,         canonical_key.length()
        ,(void *) key_val->data()        //  val data
        ,         key_val->size()
    );

    if (put_status < 0) {
        return Status::Invalid(
              "Put failed (" + std::to_string(put_status) + ") "
            + "for key: " + canonical_key
        );
    }

    // NOTE: keep track of all bytes written to kinetic
    ++keys_written;
    bytes_written += key_val->size();
    WriteData(write_logfile, key_val->size());

    return Status::OK();
}

Status
KineticConn::PutStripe( const string       &key_name
                       ,shared_ptr<Buffer>  key_val
                       ,uint8_t             stripe_size) {
    // string canonical_key { kinetic_domain->domain_key + key_name };
    string canonical_key { key_name };

    uint32_t kinetic_stripesize = stripe_size;

    int put_status = bkv_putn(
         conn_id                          // conn data
        ,(void *)  canonical_key.data()   //  key data
        ,          canonical_key.length()
        ,         &kinetic_stripesize
        ,(void *)  key_val->data()        //  val data
        ,          key_val->size()
    );

    if (put_status < 0) {
        return Status::Invalid(
              "PutStripe failed (" + std::to_string(put_status) + ") "
            + "for key: " + canonical_key
        );
    }

    // NOTE: keep track of all bytes written to kinetic
    keys_written  += stripe_size;
    bytes_written += key_val->size();
    WriteData(write_logfile, key_val->size());

    return Status::OK();
}

// NOTE: maybe should make this a shared pointer at some point
Result<unique_ptr<KBuffer>>
KineticConn::GetKey(const string& key_name) {
    // string canonical_key { kinetic_domain->domain_key + key_name };
    // string canonical_key { key_name };
    auto &&canonical_key = key_name;

    auto value_buffer = std::make_unique<KBuffer>();
    int  get_status   = bkv_get(
         conn_id                           // conn data
        ,(void *)   canonical_key.data()   //  key data
        ,           canonical_key.length()
        ,(void **) &(value_buffer->base)   //  val data
        ,          &(value_buffer->len)
    );

    if (get_status < 0) {
        return Status::Invalid(
              "Get key failed (" + std::to_string(get_status) + ") "
            + "for key: '" + canonical_key + "'"
        );
    }

    // NOTE: keep track of all bytes read from kinetic
    ++keys_read;
    bytes_read += value_buffer->len;
    WriteData(read_logfile, value_buffer->len);

    return value_buffer;
}

// NOTE: maybe should make this a shared pointer at some point
Result<unique_ptr<KBuffer>>
KineticConn::GetStripe(const string& key_name, uint8_t stripe_size) {
    // string canonical_key { kinetic_domain->domain_key + key_name };
    // string canonical_key { key_name };
    auto &&canonical_key = key_name;

    auto value_buffer = std::make_unique<KBuffer>();
    int  get_status   = bkv_getn(
         conn_id                            // conn data
        ,(void *)    canonical_key.data()   //  key data
        ,            canonical_key.length()
        ,(uint32_t)  stripe_size
        ,(void **)  &(value_buffer->base)   //  val data
        ,           &(value_buffer->len)
    );

    if (get_status < 0) {
        return Status::Invalid(
              "Get stripe failed (" + std::to_string(get_status) + ") "
            + "for key: '" + canonical_key + "'"
        );
    }

    // NOTE: keep track of all bytes read from kinetic
    keys_read  += stripe_size;
    bytes_read += value_buffer->len;
    WriteData(read_logfile, value_buffer->len);

    return value_buffer;
}


/**
 * Reads schema and metadata from the metadata key-value for this dataset partition.
 */
Result<shared_ptr<PartitionMeta>>
KineticConn::GetPartitionMeta(const string& partition_key) {
    sky_debug_printf("Querying partition metadata '%s'\n", partition_key.data());

    auto pmeta = std::make_shared<PartitionMeta>(partition_key);

    ARROW_ASSIGN_OR_RAISE(auto data_buffer, GetKey(partition_key));
    pmeta->SetSchema(std::move(data_buffer));

    return pmeta;
}


/**
 * Functions to get desired data given cluster names
 */
Result<shared_ptr<ChunkedArray>>
KineticConn::GetPartitionsByClusterNames(const StrVec& cluster_names) {
    // string clustermap_key { META_DELIM + "clusters" };
    string clustermap_key { kinetic_domain->domain_key + META_DELIM + "clusters" };

    ARROW_ASSIGN_OR_RAISE(auto pdata      , GetPartitionData(clustermap_key));
    ARROW_ASSIGN_OR_RAISE(auto cluster_map, pdata->AsTable());

    return SelectPartitionsByCluster(cluster_map, cluster_names);
}


Result<shared_ptr<Schema>>
KineticConn::GetCellMetaByClusterNames( const string& partition_key
                                       ,const StrVec& cluster_names) {
    string pclusters_key { partition_key + META_DELIM + "clusters" };
    ARROW_ASSIGN_OR_RAISE(auto pdata        , GetPartitionData(pclusters_key));
    ARROW_ASSIGN_OR_RAISE(auto cell_clusters, pdata->AsTable());

    ARROW_ASSIGN_OR_RAISE(
         shared_ptr<ChunkedArray> cell_indices
        ,SelectCellsByCluster(cell_clusters, cluster_names)
    );

    ARROW_ASSIGN_OR_RAISE(
         auto expr_meta
        ,GetPartitionMeta(partition_key)
    );

    auto expr_schema = expr_meta->schema;

    return TakeSchemaColumns(expr_schema, cell_indices);
}


Result<shared_ptr<Table>>
KineticConn::GetCellsByClusterNames( const string& partition_key
                                    ,const StrVec& cluster_names) {
    string pclusters_key { partition_key + META_DELIM + "clusters" };

    ARROW_ASSIGN_OR_RAISE(auto pdata_clusters, GetPartitionData(pclusters_key));
    ARROW_ASSIGN_OR_RAISE(auto cell_clusters , pdata_clusters->AsTable());

    ARROW_ASSIGN_OR_RAISE(
         shared_ptr<ChunkedArray> cell_indices
        ,SelectCellsByCluster(cell_clusters, cluster_names)
    );

    ARROW_ASSIGN_OR_RAISE(auto pdata_expr, GetPartitionData(partition_key));
    ARROW_ASSIGN_OR_RAISE(auto gene_expr , pdata_expr->AsTable());

    return TakeTableColumns(gene_expr, cell_indices);
}


/**
 * Writes just metadata to a key value
 */
Status
KineticConn::PutPartitionMeta( const string&                pmeta_key
                              ,      shared_ptr<Schema>     pmeta_schema
                              ,      shared_ptr<KVMetadata> pmeta_meta) {
    sky_debug_printf("Writing partition metadata '%s'\n", pmeta_key.data());

    ARROW_ASSIGN_OR_RAISE(
         shared_ptr<Buffer> batch_buffer
        ,WriteSchemaToBuffer(pmeta_schema->WithMetadata(pmeta_meta))
    );

    return PutKey(pmeta_key, batch_buffer);
}


/**
 * Reads RecordBatches from a dataset partition and merges them into a `Partition`.
 */
Result<shared_ptr<Partition>>
KineticConn::GetPartitionData(const shared_ptr<PartitionMeta>& pmeta) {
    ARROW_ASSIGN_OR_RAISE(auto stripe_size, pmeta->GetStripeSize());
    sky_debug_printf(
         "\t%ld batches striped over %d KVs\n"
        ,pmeta->slice_count
        ,pmeta->slice_width
    );

    // For each RecordBatch, accumulate a table; then return the concatenation of them
    shared_ptr<Partition> kpartition = std::make_shared<Partition>(kinetic_domain.get(), pmeta);

    for (const shared_ptr<PartitionSlice> &pslice : *kpartition) {
        ARROW_RETURN_NOT_OK(this->GetPartitionSlice(pslice, pmeta->slice_width));
    }

    return kpartition;
}


Result<shared_ptr<Partition>>
KineticConn::GetPartitionData(const string& partition_key) {
    sky_debug_printf("Querying partition '%s'\n", partition_key.data());

    // TODO: first get partition metadata
    ARROW_ASSIGN_OR_RAISE(auto pmeta, GetPartitionMeta(partition_key));
    return GetPartitionData(pmeta);
}


/**
 * Helper to asynchronously get a partition slice
 */
void
KineticConn::PromisePartition( uint8_t                      qdepth
                              ,std::vector<promise<Status>> ppromises
                              ,shared_ptr<Partition>        partition) {
    // hard coded for now
    size_t slice_startndx = 0;
    size_t slice_stopndx  = partition->meta->slice_count;

    // std::vector<std::thread> aio_queue;
    std::vector<future<void>> aio_queue;
    aio_queue.reserve(qdepth);
    
    /*
    std::cout << "Promise Partition (slices: "
              << slice_startndx << " -> " << slice_stopndx
              << ")" << std::endl
    ;
    */

    for (size_t slice_ndx = slice_startndx; slice_ndx < slice_stopndx; ++slice_ndx) {
        size_t aio_slot = slice_ndx % qdepth;

        // >> for AIO tasks
        if (slice_ndx >= qdepth and aio_queue[aio_slot].valid()) {
            aio_queue[aio_slot].get();
        }
        else if (slice_ndx >= qdepth) {
            std::cout << "AIO Task [" << slice_ndx << "] invalid" << std::endl;
        }

        // define our closure; we do here for simplicity when capturing slice_ndx
        auto io_closure = [this, slice_ndx, &ppromises, &partition] () {
            auto pslice = partition->slices[slice_ndx];

            // >> IO latency: start
            pslice->io_tstart = std::chrono::steady_clock::now();
            auto get_result = this->GetSliceData(
                pslice->key, partition->meta->slice_width
            );

            // set data on the pslice if success
            if (get_result.ok()) { pslice->SetData(std::move(*get_result)); }
            pslice->io_tstop = std::chrono::steady_clock::now();
            // << IO latency: stop

            // set the status on the promise
            ppromises[slice_ndx].set_value(get_result.status());
        };
    
        // launch AIO for a slice in a thread; decide if we push or replace
        if (slice_ndx < qdepth) { aio_queue.push_back(std::async(io_closure)); }
        else                    { aio_queue[aio_slot] = std::async(io_closure); }
    }

    // wait for the final 4 AIO slots
    for (int final_ndx = 0; final_ndx < qdepth; ++final_ndx) {
        // if (aio_queue[final_ndx].joinable()) { aio_queue[final_ndx].join(); }
        if (aio_queue[final_ndx].valid()) { aio_queue[final_ndx].get(); }
    }
}


/**
 * Asynchronously reads RecordBatches from a dataset partition and merges them into a Table.
 */
Result<shared_ptr<Partition>>
KineticConn::GetPartitionDataAsync(const shared_ptr<PartitionMeta>& pmeta, uint8_t qdepth) {
    // For each RecordBatch, accumulate a table; then return the concatenation of them
    shared_ptr<Partition> kpartition = std::make_shared<Partition>(kinetic_domain.get(), pmeta);

    // construct a promise for each partition slice, then set the slice's future
    std::vector<promise<Status>> aio_promises {};
    aio_promises.reserve(pmeta->slice_count);
    for (const auto &pslice : *kpartition) {
        promise<Status> aio_promise;

        pslice->future_result = aio_promise.get_future();
        aio_promises.push_back(std::move(aio_promise));
    }

    // thread that manages tasks for AIO for skytether partition
    std::thread kinetic_aio_thread (
         [this, qdepth, ppromises=std::move(aio_promises), kpartition]() mutable {
            this->PromisePartition(qdepth, std::move(ppromises), kpartition);
         }
    );
    kpartition->aio_thread = std::move(kinetic_aio_thread);

    return kpartition;
}


Result<shared_ptr<Partition>>
KineticConn::GetPartitionDataAsync(const string& partition_key, uint8_t qdepth) {
    // TODO: first get partition metadata
    ARROW_ASSIGN_OR_RAISE(auto pmeta, GetPartitionMeta(partition_key));

    return GetPartitionDataAsync(pmeta, qdepth);
}


/**
 * Reads RecordBatches from a single key, where a single kinetic key is a "slice" of a dataset.
 */
Result<unique_ptr<KBuffer>>
KineticConn::GetSliceData(const string &slice_key, uint8_t stripe_size) {
    if (stripe_size <= 1) {
        sky_debug_printf("Querying key '%s'\n", slice_key.data());
        return GetKey(slice_key);
    }

    sky_debug_printf("Querying stripe '%s' [%d]\n", slice_key.data(), stripe_size);
    return GetStripe(slice_key, stripe_size);
}


/**
 * Reads RecordBatches from a single key, where a single kinetic key is a "slice" of a dataset.
 */
Status
KineticConn::GetPartitionSlice(shared_ptr<PartitionSlice> pslice, uint8_t stripe_size) {
    ARROW_ASSIGN_OR_RAISE(auto slice_buffer, this->GetSliceData(pslice->key, stripe_size));
    pslice->SetData(std::move(slice_buffer));

    return Status::OK();
}


Status
KineticConn::PutPartitionSlice(const string                  &pslice_key
                               ,     shared_ptr<Schema>       pslice_schema
                               ,     shared_ptr<RecordBatch>  pslice_data
                               ,     uint8_t                  stripe_size) {
    sky_debug_printf("Writing slice '%s':\n", pslice_key.data());
    ARROW_ASSIGN_OR_RAISE(
         auto batch_buffer
        ,WriteRecordBatchToBuffer(pslice_schema, pslice_data)
    );

    // >> Send put request to kinetic
    sky_debug_printf("Writing partition slice [stripe size: %d]\n", (int) stripe_size);

    // Use different functions if we're striping vs if we're not striping.
    // This makes the distinction easily human readable
    if (stripe_size <= 1) { return PutKey(pslice_key, batch_buffer); }
    return PutStripe(pslice_key, batch_buffer, stripe_size);
}


Status
KineticConn::PutPartition( const string            &partition_key
                          ,      shared_ptr<Table>  table_data
                          ,      int64_t            byte_batch_size
                          ,      size_t             start_ndx) {

    // Grab schema with metadata first; in case `ReplaceSchemaMetadata` clobbers it
    auto schema_with_meta  = table_data->schema();
    auto schema_meta       = schema_with_meta->metadata()->Copy();
    auto table_data_nometa = table_data->ReplaceSchemaMetadata(nullptr);

    // Get stripe size from table schema; defaults to 1
    ARROW_ASSIGN_OR_RAISE(auto stripe_size, GetStripeSize(schema_meta));

    // Determine the row batch size that fits in the byte_batch_size
    ARROW_ASSIGN_OR_RAISE(
         int64_t row_batch_size
        ,RowCountForByteSize(table_data_nometa, byte_batch_size)
    );

    // If we're not happy with the row batch size; stripe slices over many KVs
    // NOTE: only do this if we're writing from the beginning; otherwise it
    // will get weird
    if (start_ndx == 0 and row_batch_size < MIN_ROWBATCH_SIZE) {
        // TODO: figure out a way to dynamically determine stripe size
        stripe_size = DEFAULT_STRIPE_SIZE;

        ARROW_ASSIGN_OR_RAISE(
             row_batch_size
            ,RowCountForByteSize(table_data_nometa, (DEFAULT_STRIPE_SIZE * byte_batch_size))
        );
    }

    arrow::TableBatchReader batch_reader(*table_data_nometa);
    batch_reader.set_chunksize(row_batch_size);

    sky_debug_printf("Reading first batch from table\n");
    shared_ptr<RecordBatch> curr_recordbatch;
    ARROW_RETURN_NOT_OK(batch_reader.ReadNext(&curr_recordbatch));

    auto partition_ndx = start_ndx;
    auto slice_schema  = schema_with_meta->RemoveMetadata();
    for (; curr_recordbatch != nullptr; partition_ndx++) {
        string slice_key {
              partition_key
            + SLICE_DELIM
            + std::to_string(partition_ndx)
        };

        sky_debug_printf("Preparing value for partition '%s':\n", slice_key.data());
        auto put_result = PutPartitionSlice(
             slice_key
            ,slice_schema
            ,curr_recordbatch
            ,stripe_size
        );

        if (not put_result.ok()) {
            std::cerr << "[PutPartition] Failed:" << put_result.message() << std::endl;
            return Status::Invalid("PutPartition: Failed to write slice");
        }

        // >> Get next record batch
        sky_debug_printf("Next record batch\n");
        ARROW_RETURN_NOT_OK(batch_reader.ReadNext(&curr_recordbatch));
    }

    // >> Set skyhook level metadata in the schema metadata
    ARROW_RETURN_NOT_OK(SetPartitionCount(schema_meta, partition_ndx));
    ARROW_RETURN_NOT_OK(SetStripeSize(schema_meta, stripe_size));

    sky_debug_block(
        PrintSchemaMetadata(schema_meta, 0, 20);
    );
    return PutPartitionMeta(partition_key, schema_with_meta, schema_meta);
}


Status
KineticConn::UnionIndex( const string            &index_key
                        ,      shared_ptr<Table>  index_data
                        ,      int64_t            byte_batch_size) {
    // Set these so that PutClusterMap works
    size_t slice_startndx = 0;
    auto   updated_index  = index_data;

    // Check if a cluster map exists, and if so, update the above variables
    Result<shared_ptr<PartitionMeta>> meta_result = GetPartitionMeta(index_key);

    // If the result is `ok()`, then data exists, and we need to append to last slice
    if (meta_result.ok()) {
        auto pmeta = meta_result.ValueOrDie();

        // Since data already exists, grab the last slice
        ARROW_ASSIGN_OR_RAISE(auto stripe_size, pmeta->GetStripeSize());
        ARROW_ASSIGN_OR_RAISE(slice_startndx  , pmeta->GetPartitionCount());
        slice_startndx -= 1;

        // grab the "tail" slice, which is the last slice (where we append)
        auto last_slice = std::make_shared<PartitionSlice>(index_key, slice_startndx);
        ARROW_RETURN_NOT_OK(GetPartitionSlice(last_slice, stripe_size));

        // Union (aka concatenate); note that this is *not* set semantics
        ARROW_ASSIGN_OR_RAISE(
             updated_index
            ,arrow::ConcatenateTables({ last_slice->data, index_data })
        );

        // combine the chunks so that we can correctly re-slice (redo the batching)
        ARROW_ASSIGN_OR_RAISE(updated_index, updated_index->CombineChunks());
    }

    sky_debug_printf("Unioning new index data\n");
    return PutPartition(index_key, updated_index, byte_batch_size, slice_startndx);
}


Status
KineticConn::UpdateIndex( const string            &index_key
                         ,      shared_ptr<Table>  index_data
                         ,      int64_t            byte_batch_size) {
    shared_ptr<Table> updated_index  = index_data;
    size_t            slice_startndx = 0;

    // Check if a cluster map exists, and if so, update the above variables
    Result<shared_ptr<PartitionMeta>> meta_result = GetPartitionMeta(index_key);

    // if `meta_result` is `ok()` then we need to grab the "tail" slice
    if (meta_result.ok()) {
        sky_debug_printf("[UpdateIndex] Updating existing index data\n");

        // The "tail" slice is the last slice of the partition
        auto pmeta = meta_result.ValueOrDie();

        ARROW_ASSIGN_OR_RAISE(slice_startndx       , pmeta->GetPartitionCount());
        ARROW_ASSIGN_OR_RAISE(auto stripe_size, pmeta->GetStripeSize());
        slice_startndx -= 1;

        auto tail_slice = std::make_shared<PartitionSlice>(index_key, slice_startndx);
        ARROW_RETURN_NOT_OK(GetPartitionSlice(tail_slice, stripe_size));

        // run simple compaction on the new cluster map
        sky_debug_printf("Running compaction\n");
        int primary_keyndx = 0;
        ARROW_ASSIGN_OR_RAISE(
             updated_index
            ,HashMergeTakeLatest(tail_slice->data, index_data, primary_keyndx)
        );

        sky_debug_printf("Compaction complete; combining chunks\n");

        // combine the chunks so that batching works as expected
        ARROW_ASSIGN_OR_RAISE(updated_index, updated_index->CombineChunks());
    }

    sky_debug_printf("Updating index data [%s]: %ld\n", index_key.data(), updated_index->num_rows());
    return PutPartition(index_key, updated_index, byte_batch_size, slice_startndx);
}


Status
KineticConn::ExtendIndex( const string            &index_key
                         ,      shared_ptr<Table>  extension_data
                         ,      int64_t            byte_batch_size) {
    // Set this early, in case there is no pre-existing index
    shared_ptr<Table> updated_index  = extension_data;
    int64_t           old_rowcount   = 0;
    int32_t           old_colcount   = 0;

    // Check for the existing index metadata
    // NOTE: this is 1 extra access, but we do it for robustness
    // auto index_result = GetPartitionData(index_key);
    auto index_exists_result = GetPartitionMeta(index_key);

    // If it exists, extend it here
    if (index_exists_result.ok()) {
        sky_debug_printf("[ExtendIndex] Updating existing index data\n");
        // NOTE: we need the actual data now
        ARROW_ASSIGN_OR_RAISE(auto partition_basendx, GetPartitionData(index_key));
        ARROW_ASSIGN_OR_RAISE(auto base_indextable  , partition_basendx->AsTable());

        old_rowcount = base_indextable->num_rows();
        old_colcount = base_indextable->num_columns();

        // Combine chunks for each table to avoid edge cases
        sky_debug_printf("[ExtendIndex] Combining column chunks\n");
        ARROW_ASSIGN_OR_RAISE(extension_data , extension_data->CombineChunks());
        ARROW_ASSIGN_OR_RAISE(base_indextable, base_indextable->CombineChunks());

        ARROW_ASSIGN_OR_RAISE(updated_index, ExtendTable(base_indextable, extension_data));

        // Combine chunks to avoid edge cases
        // ARROW_ASSIGN_OR_RAISE(updated_index, updated_index->CombineChunks());
    }

    sky_debug_printf(
         "Extending index data [%s]: <%ld, %d> (was: <%ld, %d>)\n"
        ,index_key.data()
        ,updated_index->num_rows()
        ,updated_index->num_columns()
        ,old_rowcount
        ,old_colcount
    );

    int last_colndx = updated_index->num_columns() - 1;
    sky_debug_printf(
         "\tColumn [%d] chunk count: %d\n\tColumn [%d] chunk count: %d\n"
        ,0
        ,updated_index->column(0)->num_chunks()
        ,last_colndx
        ,updated_index->column(last_colndx)->num_chunks()
    );

    // Overwrite index from first slice until we can handle independent slice schemas
    size_t slice_startndx = 0;
    return PutPartition(index_key, updated_index, byte_batch_size, slice_startndx);
}


Status
KineticConn::DropDuplicateRowsByKey(const string& partition_key, int key_ndx) {
    // Get the partition data
    ARROW_ASSIGN_OR_RAISE(auto partition     , GetPartitionData(partition_key));
    ARROW_ASSIGN_OR_RAISE(auto partition_data, partition->AsTable());

    // Dictionary encode the desired column
    ChunkedDict encoded_tablekey;
    encoded_tablekey.SetData(partition_data->column(key_ndx));

    // Keep rows where the key column had a unique value
    ARROW_ASSIGN_OR_RAISE(
         auto filtered_table
        ,SelectTableByRow(partition_data, *(encoded_tablekey.UniqueIndices()))
    );

    // Write the updated data back to kinetic; return the status
    return PutPartition(partition_key, filtered_table, MAX_KV_SIZE, 0);
}


Result<shared_ptr<Table>>
KineticConn::MapPartitionData(const string& partition_key, uint8_t qdepth) {
    sky_debug_printf("Querying partition '%s'\n", partition_key.data());

    // TODO: first get partition metadata
    ARROW_ASSIGN_OR_RAISE(auto pmeta, GetPartitionMeta(partition_key));
    sky_debug_printf(
         "\t%ld batches striped over %d KVs\n"
        ,pmeta->slice_count
        ,pmeta->slice_width
    );

    // For each RecordBatch, accumulate a table; then return the concatenation of them
    TableVec partition_batches;
    partition_batches.reserve(pmeta->slice_count);

    int64_t  expr_colstart = 1;
    uint64_t aggr_count    = 0;

    sky_debug_printf("Aggregating [%ld] slices\n", pmeta->slice_count);
    ARROW_ASSIGN_OR_RAISE(auto tbl_partition, GetPartitionDataAsync(pmeta, qdepth));

    for (size_t slice_ndx = 0; slice_ndx < pmeta->slice_count; slice_ndx++) {
        auto &pslice = tbl_partition->slices[slice_ndx];

        // wait for the async IO to complete
        if (pslice->future_result.valid()) {
            // >> IO wait: start
            pslice->wait_tstart = std::chrono::steady_clock::now();
            pslice->future_result.wait();
            pslice->wait_tstop  = std::chrono::steady_clock::now();
            // << IO wait: stop
        }
        else {
            std::cerr << "\t(sync) invalid future on slice: " << slice_ndx << std::endl;
        }

        // confirm the IO was successful
        auto get_status = pslice->future_result.get();
        if (not get_status.ok()) {
            std::cerr << "Failed to get a slice during `map`" << std::endl;
            return Result<shared_ptr<Table>>(
                Status::Invalid("Failed to get a slice during `map`")
            );
        }

        // carry on as usual

        // >> CPU time: start
        pslice->cpu_tstart = std::chrono::steady_clock::now();

        MeanAggr slice_aggr;
        slice_aggr.Accumulate(pslice->data, expr_colstart);

        // need to maintain the cardinality of the aggregate
        if (aggr_count == 0) { aggr_count = slice_aggr.count; }

        // Copy each chunk of the primary key column
        auto     pkey_chunks_src = pslice->data->column(0);
        ArrayVec pkey_chunks_copied;
        pkey_chunks_copied.reserve(pkey_chunks_src->num_chunks());

        for (int chunk_ndx = 0; chunk_ndx < pkey_chunks_src->num_chunks(); ++chunk_ndx) {
            ARROW_ASSIGN_OR_RAISE(
                 auto copied_chunk
                ,CopyStrArray(pkey_chunks_src->chunk(chunk_ndx))
            );

            pkey_chunks_copied.push_back(copied_chunk);
        }

        // gather just the aggregates and the primary key (gene IDs)
        auto aggr_data = Table::Make(
             arrow::schema({
                  arrow::field("gene_id"  , arrow::utf8())
                 ,arrow::field("means"    , arrow::float64())
                 ,arrow::field("variances", arrow::float64())
             })
            ,{
                  std::make_shared<ChunkedArray>(pkey_chunks_copied)
                 ,slice_aggr.means
                 ,slice_aggr.variances
             }
        );

        // PrintTable(aggr_data);
        partition_batches.push_back(aggr_data);

        pslice->cpu_tstop = std::chrono::steady_clock::now();
        // << CPU time: stop
    }
    tbl_partition->aio_thread.join();
    std::cout << tbl_partition->GetStats(qdepth) << std::endl;
    /* TODO:
    PutKey(
        "result/tstat."
        tbl_partition->GetStats(qdepth)
    );
    */
    // tbl_partition->WriteStats(qdepth);
    // std::cout << std::endl;

    // std::cout << "First batch; after aggregation:" << std::endl;
    // PrintTable(partition_batches[0]);

    sky_debug_printf("\tConcatenating %ld slices\n", partition_batches.size());
    auto concat_result = arrow::ConcatenateTables(partition_batches);
    if (not concat_result.ok()) {
        sky_debug_printf("Concatenation failed: %s\n", concat_result.status().message().data());
        return concat_result;
    }

    auto partition_data = concat_result.ValueOrDie();
    // PrintTable(partition_data);

    // Update the metadata, and associate it with the partition result
    ARROW_RETURN_NOT_OK(pmeta->SetAggrCount(aggr_count));
    return partition_data->ReplaceSchemaMetadata(pmeta->schema_meta);
}
