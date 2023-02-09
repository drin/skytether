/**
 * Header supporting the data model that skytether is designed around. For now, this should
 * primarily be a tabular data model backed by Apache Arrow implementations. But, this does not
 * have to always be the case.
 *
 * Author: Aldrin Montana
 */
#ifndef __DATAMODEL_TABULAR_HPP
#define __DATAMODEL_TABULAR_HPP


// ------------------------------
// Dependencies

#include <chrono>
#include <string>

#include <iostream>
#include <fstream>

#include <thread>
#include <future>


#include "datatypes.hpp"


//      |> for older c++ version, use boost library
#if USE_BOOSTFS == 1
    #include <boost/filesystem.hpp>
	namespace fs = boost::filesystem;
#else
    #include <filesystem>
	namespace fs = std::filesystem;
#endif


// ------------------------------
// Macros and aliases

using std::string;
using std::promise;
using std::future;

using clock_tick = std::chrono::time_point<std::chrono::steady_clock>;


// ------------------------------
// Constants

namespace Skytether {
    static const uint8_t DEFAULT_STRIPESIZE     = 1;

    static const string  METAKEY_PARTITIONNAME  { "skyhook_pname"      };
    static const string  METAKEY_PARTITIONCOUNT { "skyhook_pcount"     };
    static const string  METAKEY_STRIPESIZE     { "skyhook_swidth"     };
    static const string  METAKEY_AGGRCOUNT      { "skyhook_aggr_count" };

    static const string DOMAIN_DELIM { "/" };
    static const string META_DELIM   { "." };
    static const string SLICE_DELIM  { ";" };

} // `::Skytether` namespace


// ------------------------------
// Functions


namespace Skytether {
    Status SetPartitionCount(shared_ptr<KVMetadata> schema_metadata, size_t   pcount);
    Status SetStripeSize    (shared_ptr<KVMetadata> schema_metadata, uint8_t  stripe_size);
    Status SetAggrCount     (shared_ptr<KVMetadata> schema_metadata, uint64_t aggr_count);

    Result<size_t>   GetPartitionCount(shared_ptr<KVMetadata> schema_metadata);
    Result<uint8_t>  GetStripeSize    (shared_ptr<KVMetadata> schema_metadata);
    Result<uint64_t> GetAggrCount     (shared_ptr<KVMetadata> schema_metadata);

} // `::Skytether` namespace


// ------------------------------
// Structs and Classes

// >> definitions
namespace Skytether {
    /**
     * A class of objects (category) that share semantics and an identity space.
     */

    // >> forward declarations
    struct Partition;

    // >> definitions
    struct Domain {
        string domain_key;

        Domain(const string &domain_name);

        shared_ptr<Partition> PartitionFor(const string &partition_key);
    };

    /**
     * An "extent" (e.g. kinetic KeyValue) containing metadata for a partition.
     */
    struct PartitionMeta  {
        uint8_t                slice_width;
        size_t                 slice_count;
        string                 key;
        shared_ptr<Schema>     schema;
        shared_ptr<KVMetadata> schema_meta;
        unique_ptr<KBuffer>    membuffer;

        ~PartitionMeta();
        PartitionMeta(const string &);

        // disable copy constructor
        PartitionMeta(const PartitionMeta&) = delete;

        void Print(int64_t offset, int64_t length);

        Status SetSchema(shared_ptr<Schema>  data_schema);
        Status SetSchema(unique_ptr<KBuffer> data_buffer);
        Status SetPartitionCount(size_t pcount);
        Status SetStripeSize(uint8_t stripe_size);
        Status SetAggrCount(uint64_t aggr_count);

        Result<size_t>   GetPartitionCount();
        Result<uint8_t>  GetStripeSize();
        Result<uint64_t> GetAggrCount();
    };

    /** An "extent" (e.g. kinetic KeyValue) containing a subset of data for a partition. */
    struct PartitionSlice {
        size_t              index;
        string              key;

        clock_tick          cpu_tstart;
        clock_tick          cpu_tstop;
        clock_tick          io_tstart;
        clock_tick          io_tstop;
        clock_tick          wait_tstart;
        clock_tick          wait_tstop;

        shared_ptr<Table>   data;
        unique_ptr<KBuffer> membuffer;
        future<Status>      future_result; // synchronizes async IO and CPU thread

        ~PartitionSlice();
        PartitionSlice(const string &partition_key, size_t slice_ndx);

        // disable copy constructor
        PartitionSlice(const PartitionSlice&) = delete;

        inline int64_t num_rows();
        inline int     num_columns();

        void    Print(int64_t offset, int64_t length);
        void    PrintStats(std::ostream *stats_fd);
        string  GetStats();

        Status ReleaseData();
        Status SetData(shared_ptr<Table>   data_table);
        Status SetData(unique_ptr<KBuffer> data_buffer);
    };

    using PartitionData = std::vector<shared_ptr<PartitionSlice>>;

    /** Distinct objects of a domain that may or may not be from the same "dataset". */
    struct Partition {
        // >> member variables
        Domain                    *domain;
        shared_ptr<PartitionMeta>  meta;
        PartitionData              slices;
        std::thread                aio_thread;
        int64_t                    row_count;
        int                        col_count;

        ~Partition();
        Partition(Domain *domain, shared_ptr<PartitionMeta> meta);
        Partition(Partition &other)        = delete;
        Partition(const Partition &other)  = delete;
        Partition(const Partition &&other) = delete;

        // >> functions
        int64_t num_rows();
        int     num_columns();

        PartitionData::iterator       begin();
        PartitionData::iterator       end();

        PartitionData::const_iterator cbegin();
        PartitionData::const_iterator cend();

        // Functions for modifying schema
        Status SetSchema(shared_ptr<Schema>  data_schema);
        Status SetSchema(unique_ptr<KBuffer> schema_buffer);

        // Functions for modifying data
        Status ReleaseSlices();

        Status AppendSlice(shared_ptr<PartitionSlice> new_slice);
        Status SetSlice(shared_ptr<PartitionSlice> new_slice, size_t slice_ndx);

        Result<std::vector<shared_ptr<Table>>> AsSlices();
        Result<shared_ptr<Table>>              AsTable();

        void   WriteStats(uint8_t qdepth);
        string GetStats(uint8_t qdepth);

        // should represent options, but they're kind of janky right now.
        struct PartitionMapOpts {
            size_t  apply_count { 10   };
            size_t  hold_count  { 1    };
            uint8_t concurrency { 1    };
            bool    apply_inseq { true };
        };

        // Not sure how to make this general
        // Status MapSlices(std::function<> map_fn);
    };

    // >> Index types
    /** Specifies identities of particular interest. */
    struct Index {};
    struct DomainIndex    : Index {}; // indexes partitions
    struct PartitionIndex : Index {}; // indexes partition slices

} // `::Skytether` namespace


#endif // __DATAMODEL_TABULAR_HPP
