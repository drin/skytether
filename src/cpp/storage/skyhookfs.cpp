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
 * Functions and methods for reading from and writing to the local filesystem.
 */


// ------------------------------
// Dependencies

#include "../headers/skyhookfs.hpp"


// ------------------------------
// Macros and aliases

using Skytether::SLICE_DELIM;


// ------------------------------
// Functions

Result<shared_ptr<RecordBatchStreamReader>>
ReaderForIPCFile(const std::string &path_as_uri) {
    sky_debug_printf("Reading arrow IPC-formatted file: %s\n", path_as_uri.data());
    std::string path_to_file;

    // get a `FileSystem` instance (local fs scheme is "file://")
    ARROW_ASSIGN_OR_RAISE(auto localfs, arrow::fs::FileSystemFromUri(path_as_uri, &path_to_file));

    // use the `FileSystem` instance to open a handle to the file
    ARROW_ASSIGN_OR_RAISE(auto input_file_stream, localfs->OpenInputFile(path_to_file));

    // read from the handle using `RecordBatchStreamReader`
    return RecordBatchStreamReader::Open(
         input_file_stream
        ,IPCReadOpts::Defaults()
    );
}


Result<shared_ptr<RecordBatchStreamReader>>
ReaderForPartition(const std::string &source_table, size_t partition_ndx) {
    fs::path path_to_partition {
        fs::absolute(
              source_table
            + SLICE_DELIM
            + std::to_string(partition_ndx)
            + arrow_ext
        )
    };

    if (fs::exists(path_to_partition)) {
        return ReaderForIPCFile(local_file_protocol + path_to_partition.string());
    }

    return Result<shared_ptr<RecordBatchStreamReader>>(
        arrow::Status::Invalid("Partition does not exist")
    );
}


Result<shared_ptr<Table>>
ReadIPCFile(const std::string& path_to_file) {
    sky_debug_printf("Parsing file: '%s'\n", path_to_file.c_str());

    // Declares and initializes `batch_reader`
    ARROW_ASSIGN_OR_RAISE(auto batch_reader, ReaderForIPCFile(path_to_file));

    return arrow::Table::FromRecordBatchReader(batch_reader.get());
}


arrow::Status
AppendFromIPCFile(TableVec &table_vec, const std::string &path_to_file) {
    sky_debug_printf("Parsing file: '%s'\n", path_to_file.data());

    // Declares and initializes `batch_reader`
    ARROW_ASSIGN_OR_RAISE(auto batch_reader, ReaderForIPCFile(path_to_file));

    // Declares and initializes `batches_as_table`
    ARROW_ASSIGN_OR_RAISE(
         auto batches_as_table
        ,arrow::Table::FromRecordBatchReader(batch_reader.get())
    );

    table_vec.push_back(batches_as_table);
    return arrow::Status::OK();
}


// ------------------------------
// Writers

// >> Construct writer objects
Result<shared_ptr<RecordBatchWriter>>
WriterForIPCFile(shared_ptr<Schema> schema, const std::string &path_as_uri) {
    sky_debug_printf("Writing arrow IPC-formatted file: %s", path_as_uri.c_str());

    std::string path_to_file;

    // get a `FileSystem` instance (local fs scheme is "file://")
    ARROW_ASSIGN_OR_RAISE(auto localfs, arrow::fs::FileSystemFromUri(path_as_uri, &path_to_file));

    // create a handle for the file (expecting a RandomAccessFile type)
    ARROW_ASSIGN_OR_RAISE(auto output_file_stream, localfs->OpenOutputStream(path_to_file));

    // read from the handle using `RecordBatchStreamReader`
    return arrow::ipc::MakeFileWriter(
        output_file_stream, schema, IPCWriteOpts::Defaults()
    );
}


Result<shared_ptr<RecordBatchWriter>>
WriterForPartition(shared_ptr<Schema> schema, const std::string &path_prefix, size_t partition_ndx) {
    fs::path path_to_partition {
        fs::absolute(
              path_prefix
            + SLICE_DELIM
            + std::to_string(partition_ndx)
            + arrow_ext
        )
    };

    return WriterForIPCFile(schema, local_file_protocol + path_to_partition.string());
}


// >> Persist to storage
arrow::Status
WriteBufferToFile(shared_ptr<Buffer> data_buffer, const std::string &output_filepath) {
    std::ofstream output_fd(output_filepath, std::ios::binary | std::ios::out);

    output_fd.write(
         reinterpret_cast<const char *>(data_buffer->data())
        ,data_buffer->size()
    );
    output_fd.close();

    return arrow::Status::OK();
}


// TODO: change param order to match KineticConn::PutTableBatches
arrow::Status
WriteAsRecordBatches( shared_ptr<Table> table_data
                     ,const std::string &path_prefix
                     ,int64_t batch_size) {
    auto table_schema = table_data->schema();

    // Create a TableBatchReader to convert into RecordBatches
    arrow::TableBatchReader batch_reader(*table_data);
    batch_reader.set_chunksize(batch_size);

    // Walk the table in batches and write each batch
    sky_debug_printf("Iterating over first record batch\n");
    shared_ptr<RecordBatch> curr_recordbatch;
    ARROW_RETURN_NOT_OK(batch_reader.ReadNext(&curr_recordbatch));

    for (size_t partition_ndx = 0; curr_recordbatch != nullptr; partition_ndx++) {
        // Create a writer that uses the output stream from the filesystem
        sky_debug_printf("Creating writer for partition %lu\n", partition_ndx);
        ARROW_ASSIGN_OR_RAISE(
             auto partition_writer
            ,WriterForPartition(table_schema, path_prefix, partition_ndx)
        );

        // Use writer to write RecordBatch to the partition buffer
        sky_debug_printf("Writing record batch\n");
        ARROW_RETURN_NOT_OK(partition_writer->WriteRecordBatch(*curr_recordbatch));
        ARROW_RETURN_NOT_OK(partition_writer->Close());

        // Grab next recordbatch
        sky_debug_printf("Next record batch\n");
        ARROW_RETURN_NOT_OK(batch_reader.ReadNext(&curr_recordbatch));
    }

    return arrow::Status::OK();
}
