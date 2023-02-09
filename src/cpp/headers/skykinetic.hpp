/**
 */

#ifndef __SKYKINETIC_HPP
#define __SKYKINETIC_HPP


// ------------------------------
// Dependencies

#include "skytether.hpp"

// NOTE: for hard-coded short term data
#include "operators.hpp"

extern "C" {
    #include <kinetic/basickv.h>
}


// ------------------------------
// Macros and aliases

using std::string;

using Skytether::Domain;
using Skytether::PartitionMeta;
using Skytether::PartitionSlice;
using Skytether::Partition;


// ------------------------------
// Structs and classes

// reference constants for connections
extern const string default_host;
extern const string default_port;
extern const string default_pass;


struct KineticConn {

    // >> Attributes
    // purely for generating a graph one time
    string write_logfile { "bytes-written.log" };
    string read_logfile  { "bytes-read.log"    };

    size_t keys_written  { 0 };
    size_t keys_read     { 0 };
    size_t bytes_written { 0 };
    size_t bytes_read    { 0 };

    int                conn_id;
    shared_ptr<Domain> kinetic_domain;

    // >> Functions

    ~KineticConn();
    KineticConn();
    KineticConn(const std::string &domain_name);

    bool IsConnected();
    void Disconnect();
    void Connect(
         const string &host  =default_host
        ,const string &port  =default_port
        ,const string &passwd=default_pass
    );

    void PrintStats();

    // >> lower-level API
    Status
    PutKey(const string &key_name, shared_ptr<Buffer> key_val);

    Status
    PutStripe(const string &key_name, shared_ptr<Buffer> key_val, uint8_t stripe_size);

    Result<unique_ptr<KBuffer>> GetKey   (const string &key_name);
    Result<unique_ptr<KBuffer>> GetStripe(const string &key_name, uint8_t stripe_size);

    // >> high-level API

    //  |> Promise-based functions
    void
    PromisePartition( uint8_t                      qdepth
                     ,std::vector<promise<Status>> ppromises
                     ,shared_ptr<Partition>        partition);

    // |> Synchronous accessor functions
    Result<shared_ptr<PartitionMeta>>
    GetPartitionMeta (const string &partition_key);

    Result<shared_ptr<Partition>>
    GetPartitionData (const shared_ptr<PartitionMeta>& pmeta);

    Result<shared_ptr<Partition>>
    GetPartitionData (const string &partition_key);

    Result<unique_ptr<KBuffer>>
    GetSliceData(const string &slice_key, uint8_t stripe_size);

    Status
    GetPartitionSlice(shared_ptr<PartitionSlice> slice, uint8_t stripe_size);

    Result<shared_ptr<ChunkedArray>>
    GetPartitionsByClusterNames(const StrVec &cluster_names);

    Result<shared_ptr<Schema>>
    GetCellMetaByClusterNames(const string &partition_key, const StrVec& cluster_names);

    Result<shared_ptr<Table>>
    GetCellsByClusterNames(const string &partition_key, const StrVec &cluster_names);

    Status
    PutPartitionMeta(
         const string                 &pmeta_key
        ,      shared_ptr<Schema>      pmeta_schema
        ,      shared_ptr<KVMetadata>  pmeta_meta
    );

    Status
    PutPartitionSlice(
         const string                  &pslice_key
        ,      shared_ptr<Schema>       pslice_schema
        ,      shared_ptr<RecordBatch>  pslice_data
        ,      uint8_t                  stripe_size
    );

    Status
    PutPartition(
         const string            &table_key
        ,      shared_ptr<Table>  table_data
        ,      int64_t            byte_batch_size
        ,      size_t             start_ndx = 0
    );

    // |> Async accessor functions
    Result<shared_ptr<Partition>>
    GetPartitionDataAsync(const string& partition_key, uint8_t qdepth = 4);

    Result<shared_ptr<Partition>>
    GetPartitionDataAsync(const shared_ptr<PartitionMeta>& partition_key, uint8_t qdepth = 4);

    // |> Synchronous misc functions
    Status
    UnionIndex(
         const string            &index_key
        ,      shared_ptr<Table>  index_data
        ,      int64_t            byte_batch_size
    );

    Status
    UpdateIndex(
         const string            &index_key
        ,      shared_ptr<Table>  index_data
        ,      int64_t            byte_batch_size
    );

    Status
    ExtendIndex(
         const string            &index_key
        ,      shared_ptr<Table>  extension_data
        ,      int64_t            byte_batch_size
    );

    Status
    DropDuplicateRowsByKey(const string &partition_key, int key_ndx);


    // >> for hard-coded short term code
    Result<shared_ptr<Table>>
    MapPartitionData(const string& partition_key, uint8_t qdepth);
};


// ------------------------------
// API

Result<int64_t>
RowCountForByteSize(
     shared_ptr<Table> table_data
    ,int64_t           target_bytesize
    ,int64_t           row_batchsize = 20480
);


#endif // __SKYKINETIC_HPP
