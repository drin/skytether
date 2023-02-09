/**
 * Overall header for this library
 * Author: Aldrin Montana
 */

// we want a name that's not likely to collide with others'
#ifndef __SKYTETHER_FS_HPP
#define __SKYTETHER_FS_HPP


// ------------------------------
// Dependencies

// >> Standard library dependencies

/* Std lib includes */
#include <fstream>

//      |> for older c++ version, use boost library
#if USE_BOOSTFS == 1
    #include <boost/filesystem.hpp>
	namespace fs = boost::filesystem;
#else
    #include <filesystem>
	namespace fs = std::filesystem;
#endif

// >> 3rd party dependencies
#include <arrow/filesystem/api.h>

// >> local dependencies
#include "skytether.hpp"


// ------------------------------
// Macros and aliases

using arrow::io::RandomAccessFile;
using arrow::fs::FileSystem;


// ------------------------------
// Constants

const std::string local_file_protocol { "file://" };

const std::string arrow_ext           { ".arrow"   };
const std::string kinetic_ext         { ".kinetic" };


// ------------------------------
// API

// >> Readers
//      |> Construct reader objects
Result<shared_ptr<RecordBatchStreamReader>>
ReaderForIPCFile(const std::string &path_as_uri);

Result<shared_ptr<RecordBatchStreamReader>>
ReaderForPartition(const std::string &source_table, size_t partition_ndx = 0);

//      |> Construct data objects
Result<shared_ptr<Table>>
ReadIPCFile(const std::string &path_to_file);

//      |> Operate on data objects
arrow::Status
AppendFromIPCFile(TableVec &table_vec, const std::string &path_to_file);


// >> Writers
//      |> Construct writer objects
Result<shared_ptr<RecordBatchWriter>>
WriterForIPCFile(shared_ptr<Schema> schema, const std::string &path_as_uri);

Result<shared_ptr<RecordBatchWriter>>
WriterForPartition(shared_ptr<Schema> schema, const std::string &path_prefix, size_t partition_ndx = 0);

//      |> Persist to storage
arrow::Status
WriteBufferToFile(shared_ptr<Buffer> data_buffer, const std::string &output_filepath);

arrow::Status
WriteAsRecordBatches(shared_ptr<Table> table_data, const std::string &path_prefix, int64_t batch_size);


#endif // __SKYTETHER_FS_HPP
