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
 * Implementations for tabular classes that form the basis of skytether and skyhook design. I
 * expect this will primarily be definitional (classes or data types), with minimal functions or
 * operators. I would like most functionality to be implemented by domain-specific code, such as
 * for single-cell gene expression. The functions defined here should primarily be structural, such
 * as iterators, delegation, etc.
 */


// ------------------------------
// Dependencies

#include "../headers/datamodel.hpp"


// ------------------------------
// Functions

namespace Skytether {

    std::string
    TickToMS(clock_tick tick_val) {
        auto&& tick_tstamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            tick_val.time_since_epoch()
        );

        // NOTE: using string as return type is easier than
        //       figuring out what type `count()` returns
        return std::to_string(tick_tstamp.count());
    }

    template<typename SizeType>
    SizeType
    DecodeNumeric(string encoded_numeric) {
        uint8_t *bytes = (uint8_t *) encoded_numeric.data();

        SizeType decoded_val   = 0;
        int      dest_bitshift = 0;
        for (size_t src_bytendx = encoded_numeric.size(); src_bytendx > 0; src_bytendx--) {
            decoded_val   += (SizeType) bytes[src_bytendx - 1] << dest_bitshift;
            dest_bitshift += 8;
        }

        return decoded_val;
    }

    template<typename SizeType>
    string
    EncodeNumeric(SizeType numeric_val) {
        // just to be robust
        size_t   size_size  = sizeof(SizeType);
        uint8_t *bytes      = new uint8_t[size_size];
        int      bit_offset = 0;

        size_t upper_mask = 0xFF;
        for (size_t byte_pos = size_size; byte_pos > 0; byte_pos--) {
            bytes[byte_pos - 1]  = (uint8_t) ((numeric_val >> bit_offset) & upper_mask);
            bit_offset          += 8;
        }

        return string((char *) bytes, size_size);
    }

    Status SetPartitionCount(shared_ptr<KVMetadata> schema_metadata, size_t pcount) {
        if (schema_metadata == nullptr) {
            return Status::Invalid("Cannot set `partition_count` on null KVMetadata");
        }

        return schema_metadata->Set(
             METAKEY_PARTITIONCOUNT
            ,EncodeNumeric<size_t>(pcount)
        );
    }

    Status SetStripeSize(shared_ptr<KVMetadata> schema_metadata, uint8_t stripe_size) {
        if (schema_metadata == nullptr) {
            return Status::Invalid("Cannot set `stripe_size` on null KVMetadata");
        }

        return schema_metadata->Set(
             METAKEY_STRIPESIZE
            ,EncodeNumeric<uint8_t>(stripe_size)
        );
    }

    Status SetAggrCount(shared_ptr<KVMetadata> schema_metadata, uint64_t aggr_count) {
        if (schema_metadata == nullptr) {
            return Status::Invalid("Cannot set `aggr_count` on null KVMetadata");
        }

        return schema_metadata->Set(
             METAKEY_AGGRCOUNT
            ,EncodeNumeric<uint64_t>(aggr_count)
        );
    }

    Result<size_t> GetPartitionCount(shared_ptr<KVMetadata> schema_metadata) {
        if (schema_metadata == nullptr) {
            return arrow::Status::Invalid("Cannot get `partition_count` from null KVMetadata");
        }

        auto pcount_result = schema_metadata->Get(METAKEY_PARTITIONCOUNT);
        if (not pcount_result.ok()) {
            pcount_result = schema_metadata->Get("partition_count");

            if (not pcount_result.ok()) {
                return Result<size_t>(Status::Invalid("Could not decode `partition_count`"));
            }
        }
        string encoded_pcount = *pcount_result;

        /*
        ARROW_ASSIGN_OR_RAISE(
             string encoded_pcount
            ,schema_metadata->Get(METAKEY_PARTITIONCOUNT)
        );
        */

        if (encoded_pcount.size() > sizeof(size_t)) {
            return Status::Invalid("Expected encoded partition count to be <= 8 bytes");
        }

        return DecodeNumeric<size_t>(encoded_pcount);
    }

    Result<uint8_t> GetStripeSize(shared_ptr<KVMetadata> schema_metadata) {
        if (schema_metadata == nullptr) { return Result<uint8_t>(DEFAULT_STRIPESIZE); }

        auto get_result = schema_metadata->Get(METAKEY_STRIPESIZE);
        if (not get_result.ok()) { return Result<uint8_t>(DEFAULT_STRIPESIZE); }

        // decode the value
        string encoded_stripe_size = get_result.ValueOrDie();
        if (encoded_stripe_size.size() > sizeof(uint8_t)) {
            return Status::Invalid("Expected encoded partition count to be <= 1 bytes");
        }

        return DecodeNumeric<uint8_t>(encoded_stripe_size);
    }


    Result<uint64_t> GetAggrCount(shared_ptr<KVMetadata> schema_metadata) {
        if (schema_metadata == nullptr) {
            return arrow::Status::Invalid("Cannot get `partition_count` from null KVMetadata");
        }

        ARROW_ASSIGN_OR_RAISE(
             string encoded_aggr_count
            ,schema_metadata->Get(METAKEY_AGGRCOUNT)
        );

        if (encoded_aggr_count.size() > sizeof(uint64_t)) {
            return Status::Invalid("Expected encoded aggregate count to be <= 8 bytes");
        }

        return DecodeNumeric<uint64_t>(encoded_aggr_count);
    }

} // `::Skytether` namespace


// ------------------------------
// Structs and Classes

// Partitions
namespace Skytether {

    // >> Domain
    Domain::Domain(const string &domain_name) {
        this->domain_key = domain_name;
    }

    shared_ptr<Partition>
    Domain::PartitionFor(const string &partition_key) {
        string meta_pkey { domain_key + DOMAIN_DELIM + partition_key };
        return std::make_shared<Partition>(this, std::make_shared<PartitionMeta>(meta_pkey));
    }

    // >> PartitionMeta
    PartitionMeta::~PartitionMeta() {
        schema.reset();
        schema_meta.reset();
        membuffer.reset(nullptr);
    }

    PartitionMeta::PartitionMeta(const string &partition_key) {
        key         = partition_key;
        membuffer   = unique_ptr<KBuffer>(nullptr);
        schema      = shared_ptr<Schema>(nullptr);
        schema_meta = shared_ptr<KVMetadata>(nullptr);
    }

    void PartitionMeta::Print(int64_t offset, int64_t length) {
        /*
        PrintSchemaAttributes(schema, offset, length);
        PrintSchemaMetadata(schema_meta, offset, length);
        */
    }

    Status PartitionMeta::SetSchema(shared_ptr<Schema> data_schema) {
        schema = data_schema;

        if (not schema->HasMetadata()) { schema_meta = std::make_shared<KVMetadata>(); }
        else                           { schema_meta = schema->metadata()->Copy();     }

        // cache some values
        auto pcount_result = this->GetPartitionCount();
        if (pcount_result.ok()) { this->slice_count = *pcount_result; }

        auto stripe_result = this->GetStripeSize();
        if (stripe_result.ok()) { this->slice_width = *stripe_result; }

        return Status::OK();
    }

    Status PartitionMeta::SetSchema(unique_ptr<KBuffer> schema_buffer) {
        // If deserializing arrow from KBuffer fails; do empty init
        auto reader_result = schema_buffer->NewRecordBatchReader();
        if (not reader_result.ok()) {
            schema_buffer.reset();
            return Status::Invalid("Unable to construct reader for schema buffer");
        }

        // Otherwise; set all pointers as appropriate
        membuffer = std::move(schema_buffer);
        return this->SetSchema((*reader_result)->schema());
    }

    Status PartitionMeta::SetPartitionCount(size_t pcount) {
        return Skytether::SetPartitionCount(schema_meta, pcount);
    }

    Status PartitionMeta::SetStripeSize(uint8_t stripe_size) {
        return Skytether::SetStripeSize(schema_meta, stripe_size);
    }

    Status PartitionMeta::SetAggrCount(uint64_t aggr_count) {
        return Skytether::SetAggrCount(schema_meta, aggr_count);
    }

    Result<size_t> PartitionMeta::GetPartitionCount() {
        return Skytether::GetPartitionCount(schema_meta);
    }

    Result<uint8_t> PartitionMeta::GetStripeSize() {
        return Skytether::GetStripeSize(schema_meta);
    }

    Result<uint64_t> PartitionMeta::GetAggrCount() {
        return Skytether::GetAggrCount(schema_meta);
    }

    // >> PartitionSlice
    PartitionSlice::~PartitionSlice() {
        data.reset();
        membuffer.reset(nullptr);

        if (future_result.valid()) { future_result.get(); }
    }

    PartitionSlice::PartitionSlice(const string &partition_key, size_t slice_ndx) {
        index     = slice_ndx;
        key       = string { partition_key + SLICE_DELIM + std::to_string(slice_ndx) };

        membuffer = unique_ptr<KBuffer>(nullptr);
        data      = shared_ptr<Table>(nullptr);

    }

    inline int64_t PartitionSlice::num_rows()    { return data->num_rows(); }
    inline int     PartitionSlice::num_columns() { return data->num_columns(); }

    void PartitionSlice::Print(int64_t offset, int64_t length) {
        // PrintTable(data, offset, length);
    }

    void PartitionSlice::PrintStats(std::ostream *stats_fd) {
        if (stats_fd == nullptr) { stats_fd = &(std::cout); }

        (*stats_fd) <<         std::to_string(index)
                    << "\t" << TickToMS(  io_tstart)
                    << "\t" << TickToMS(   io_tstop)
                    << "\t" << TickToMS(wait_tstart)
                    << "\t" << TickToMS( wait_tstop)
                    << "\t" << TickToMS( cpu_tstart)
                    << "\t" << TickToMS(  cpu_tstop)
                    <<         std::endl
        ;
    }

    string PartitionSlice::GetStats() {
        return string {
              std::to_string(index)
            + "\t" + TickToMS(  io_tstart)
            + "\t" + TickToMS(   io_tstop)
            + "\t" + TickToMS(wait_tstart)
            + "\t" + TickToMS( wait_tstop)
            + "\t" + TickToMS( cpu_tstart)
            + "\t" + TickToMS(  cpu_tstop)
            + "\n"
        };
    }

    Status PartitionSlice::ReleaseData() {
        data.reset();
        membuffer.reset(nullptr);

        return Status::OK();
    }

    Status PartitionSlice::SetData(shared_ptr<Table> data_table) {
        data = data_table;

        return Status::OK();
    }

    Status PartitionSlice::SetData(unique_ptr<KBuffer> data_buffer) {
        // If deserializing arrow from KBuffer fails; do empty init
        auto reader_result = data_buffer->NewRecordBatchReader();
        if (not reader_result.ok()) {
            data_buffer.reset();
            return Status::Invalid("Unable to construct reader for slice buffer");
        }

        // Otherwise; set all pointers as appropriate
        membuffer = std::move(data_buffer);

        shared_ptr<RecordBatchStreamReader> buffer_reader = reader_result.ValueOrDie();
        ARROW_ASSIGN_OR_RAISE(data, Table::FromRecordBatchReader(buffer_reader.get()));

        return Status::OK();
    }

    // >> Partition implementations
    Partition::~Partition() {
        domain = nullptr;

        meta.reset();
        slices.clear();

        if (aio_thread.joinable()) { aio_thread.join(); }
    }

    Partition::Partition(Domain *domain, shared_ptr<PartitionMeta> meta) {
        this->domain    = domain;
        this->meta      = meta;

        this->row_count = 0;
        this->col_count = 0;

        auto pcount_result = meta->GetPartitionCount();
        if (pcount_result.ok()) {
            auto pcount_val = pcount_result.ValueOrDie();

            slices.reserve(pcount_val);
            for (size_t slice_ndx = 0; slice_ndx < pcount_val; ++slice_ndx) {
                slices.push_back(std::make_shared<PartitionSlice>(meta->key, slice_ndx));
            }
        }
    }

    int64_t Partition::num_rows()    {
        if (row_count == 0) { row_count = slices[0]->num_rows(); }

        return row_count;
    }

    int Partition::num_columns() {
        if (col_count == 0) { col_count = slices[0]->num_columns(); }

        return col_count;
    }

    PartitionData::iterator       Partition::begin()  { return slices.begin();  }
    PartitionData::iterator       Partition::end()    { return slices.end();    }

    PartitionData::const_iterator Partition::cbegin() { return slices.cbegin(); }
    PartitionData::const_iterator Partition::cend()   { return slices.cend();   }

    Status Partition::SetSchema(shared_ptr<Schema> data_schema) {
        return meta->SetSchema(data_schema);
    }

    Status Partition::SetSchema(unique_ptr<KBuffer> schema_buffer) {
        return meta->SetSchema(std::move(schema_buffer));
    }

    Status Partition::ReleaseSlices() {
        if (slices.size() > 0) { slices.clear(); } 
        return Status::OK();
    }

    Status Partition::AppendSlice(shared_ptr<PartitionSlice> new_slice) {
        slices.push_back(new_slice);
        return Status::OK();
    }

    Status Partition::SetSlice(shared_ptr<PartitionSlice> new_slice, size_t slice_ndx) {
        if (slice_ndx > slices.size()) {
            return Status::Invalid("Unable to set slice for index");
        }

        slices[slice_ndx] = new_slice;
        return Status::OK();
    }

    Result<std::vector<shared_ptr<Table>>> Partition::AsSlices() {
        std::vector<shared_ptr<Table>> pslices;
        pslices.reserve(this->slices.size());
        for (const auto &pslice : this->slices) { pslices.push_back(pslice->data); }

        return pslices;
    }

    Result<shared_ptr<Table>> Partition::AsTable() {
        std::vector<shared_ptr<Table>> slice_data;
        slice_data.reserve(this->slices.size());
        for (const auto &pslice : this->slices) { slice_data.push_back(pslice->data); }

        auto concat_result = arrow::ConcatenateTables(slice_data);
        if (not concat_result.ok()) { return concat_result; }

        // Make sure we also return metadata
        auto partition_data = concat_result.ValueOrDie();
        return partition_data->ReplaceSchemaMetadata(this->meta->schema_meta);
    }

    string Partition::GetStats(uint8_t qdepth) {
        string stats_log {
            "slice ID"
            "\t" "IO start"
            "\t" "IO stop"
            "\t" "wait start"
            "\t" "wait stop"
            "\t" "cpu start"
            "\t" "cpu stop"
            "\n"
        };

        for (const auto &pslice : this->slices) {
            stats_log += pslice->GetStats();
        }

        return stats_log;
    }

    void Partition::WriteStats(uint8_t qdepth) {
        fs::path stats_fpath {
              "stats/qdepth-"
            + std::to_string(qdepth)
            + "/" + meta->key + ".log"
        };
        fs::create_directories(stats_fpath.parent_path());

        auto did_pathexist = fs::exists(stats_fpath);
        std::ofstream stats_fd { stats_fpath.string(), std::ios::app | std::ios::out };

        if (not did_pathexist) {
            stats_fd << "slice ID"
                     << "\t" << "IO start"
                     << "\t" << "IO stop"
                     << "\t" << "wait start"
                     << "\t" << "wait stop"
                     << "\t" << "cpu start"
                     << "\t" << "cpu stop"
                     << std::endl
            ;
        }
        for (const auto &pslice : this->slices) { pslice->PrintStats(&(stats_fd)); }

        stats_fd.close();
    }

} // `::Skytether` namespace
