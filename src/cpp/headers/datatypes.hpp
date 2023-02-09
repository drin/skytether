/**
 * Header for format-specific definitions used by this library.
 * Author: Aldrin Montana
 */
#ifndef __DATAFORMATS_HPP
#define __DATAFORMATS_HPP


// ------------------------------
// Dependencies

/* Arrow dependencies */
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/dataset/api.h>
#include <arrow/compute/api.h>


// ------------------------------
// Type aliases for readability

// >> Memory management types
using std::shared_ptr;
using std::unique_ptr;

using arrow::Buffer;
using arrow::io::InputStream;
using arrow::io::BufferReader;
using arrow::io::OutputStream;
using arrow::io::BufferOutputStream;

// >> Result handling types
using arrow::Result;
using arrow::Status;

// >> Data description types
using arrow::DataType;
using arrow::Field;
using arrow::Schema;

// >> Array types
using arrow::Array;
using arrow::ChunkedArray;
using arrow::Int32Array;
using arrow::Int64Array;
using arrow::StringArray;
using arrow::DictionaryArray;

// >> Tabular types
using arrow::Table;
using arrow::RecordBatch;

// >> Container types
using arrow::Datum;

using KVMetadata = arrow::KeyValueMetadata;

// >> Reader/writer types
using arrow::TableBatchReader;
using arrow::RecordBatchReader;

using arrow::ipc::RecordBatchWriter;
using arrow::ipc::RecordBatchStreamReader;

// >> Data processing types

using arrow::compute::Expression;
using arrow::dataset::InMemoryDataset;
using arrow::dataset::ScannerBuilder;
using arrow::dataset::Scanner;

// >> Option types
using IPCReadOpts  = arrow::ipc::IpcReadOptions ;
using IPCWriteOpts = arrow::ipc::IpcWriteOptions;

// >> Collection types
using IndexVec       = std::vector<size_t>;
using Int32Vec       = std::vector<int32_t>;
using Int64Vec       = std::vector<int64_t>;
using StrVec         = std::vector<std::string>;
using StrTable       = std::vector<StrVec>;

using ArrayVec       = arrow::ArrayVector;
using ChunkedArrVec  = arrow::ChunkedArrayVector;
using FieldVec       = arrow::FieldVector;
using RecordBatchVec = arrow::RecordBatchVector;
using TableVec       = std::vector<shared_ptr<Table>>;


// ------------------------------
// Global Variables


// ------------------------------
// Internal Data Structures

struct KBuffer {
    uint8_t *base;
    size_t   len;

    ~KBuffer();
    KBuffer();
    KBuffer(uint8_t *buf_base, size_t buf_len);

    std::string ToString();

    void Release();
    bool Matches(char *other);
    bool Matches(std::string other);
    bool Matches(KBuffer *other);

    Result<shared_ptr<RecordBatchStreamReader>>
    NewRecordBatchReader();
};


// ------------------------------
// API

//    > Utils
arrow::Result<shared_ptr<ChunkedArray>>
UnionColumnByKey(shared_ptr<Table> left_table, shared_ptr<Table> right_table, int key_ndx);


#endif // __DATAFORMATS_HPP
