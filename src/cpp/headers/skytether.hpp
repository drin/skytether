/**
 * Overall header for this library
 * Author: Aldrin Montana
 */

#ifndef __SKYTETHER_HPP
#define __SKYTETHER_HPP


// ------------------------------
// Dependencies

/* C includes */
#include <stdint.h>
#include <stdio.h>

#include <string>
#include <vector>
#include <iostream>

#include <thread>
#include <future>

/* Internal includes */
#include "datatypes.hpp"
#include "datamodel.hpp"

// ------------------------------
// Macros and aliases

// NOTE: the meson build file should set this, but I'll leave this here if someone wants to set it
// manually
// #define SKYTETHER_DEBUG 1
// #define USE_BOOSTFS 1

#if SKYTETHER_DEBUG == 0
    #define sky_debug_printf(...)        {}
    #define sky_debug_block(blk_content) {}

#else
    #define sky_debug_printf(...)        printf(__VA_ARGS__)
    #define sky_debug_block(blk_content) { blk_content }

#endif

/*
#if PROFILE != 0
#else
    #define PROFILE_addio(...)        {}
    #define sky_debug_block(blk_content) {}

#endif
*/


using Skytether::Partition;


// ------------------------------
// Constants


// ------------------------------
// Common API

// >> Convenience Functions
void PrintStrVec(StrVec record_data);
void PrintStrTable(StrTable filedata);

void PrintSchemaAttributes(shared_ptr<Schema>     , int64_t offset=0, int64_t length=10);
void PrintSchemaMetadata  (shared_ptr<KVMetadata> , int64_t offset=0, int64_t length=10);
void PrintSchema          (shared_ptr<Schema>     , int64_t offset=0, int64_t length=10);
void PrintTable           (shared_ptr<Table>      , int64_t offset=0, int64_t length=10);
void PrintBatch           (shared_ptr<RecordBatch>, int64_t offset=0, int64_t length=10);

// A facade until I have time to refactor further
void PrintTable           (shared_ptr<Partition>, int64_t offset=0, int64_t length=10);


// >> Arrow related utility functions
Result<shared_ptr<KVMetadata>> MetadataFromSchema(shared_ptr<Schema> schema);

Result<shared_ptr<Array>>      EmptyDoubles(int64_t val_count);
Result<shared_ptr<Array>>      CopyStrArray(shared_ptr<Array> src_array);

Result<shared_ptr<ScannerBuilder>>
ScannerForBatches(shared_ptr<Schema> data_schema, RecordBatchVec record_batches);

Status
append_batches(TableVec table_vec, std::string& path_to_file);


#endif // __SKYTETHER_HPP
