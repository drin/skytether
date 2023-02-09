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
 * Functions and methods for reading data from various sources, or for returning reader objects.
 */


// ------------------------------
// Dependencies

#include "../headers/operators.hpp"


// ------------------------------
// Functions

// >> Serialize arrow structures to data buffers
Result<shared_ptr<Buffer>>
WriteSchemaToBuffer(shared_ptr<Schema> schema) {
    // >> Create a buffer stream and a writer that writes to it
    ARROW_ASSIGN_OR_RAISE(auto batch_bufstream, BufferOutputStream::Create());

    // >> Create a writer for the buffer with the given schema
    ARROW_ASSIGN_OR_RAISE(
         auto batch_writer
        ,MakeStreamWriter(batch_bufstream, schema, IPCWriteOpts::Defaults())
    );

    // >> The writer writes the schema itself, so we just close and clean up
    ARROW_RETURN_NOT_OK(batch_writer->Close());

    // >> Then; close, finish, and return the buffer
    if (not batch_bufstream->closed()) { batch_bufstream->Close(); }
    return batch_bufstream->Finish();
}


Result<shared_ptr<Buffer>>
WriteRecordBatchToBuffer(shared_ptr<Schema> schema, shared_ptr<RecordBatch> record_batch) {
    // >> Create a buffer stream and a writer that writes to it
    ARROW_ASSIGN_OR_RAISE(auto batch_bufstream, BufferOutputStream::Create());

    // >> Create a writer for the buffer with the given schema
    ARROW_ASSIGN_OR_RAISE(
         auto batch_writer
        ,MakeStreamWriter(batch_bufstream, schema, IPCWriteOpts::Defaults())
    );

    // >> Write batch to stream
    ARROW_RETURN_NOT_OK(batch_writer->WriteRecordBatch(*record_batch));

    // >> Close the writer
    ARROW_RETURN_NOT_OK(batch_writer->Close());

    // >> Then; close, finish, and return the buffer
    if (not batch_bufstream->closed()) { batch_bufstream->Close(); }
    return batch_bufstream->Finish();
}


Result<shared_ptr<Buffer>>
WriteTableToBuffer(shared_ptr<Table> table) {
    // >> Create a buffer stream and a writer that writes to it
    ARROW_ASSIGN_OR_RAISE(auto batch_bufstream, BufferOutputStream::Create());

    // >> Create a writer for the buffer with the given schema
    ARROW_ASSIGN_OR_RAISE(
         auto batch_writer
        ,MakeStreamWriter(batch_bufstream, table->schema(), IPCWriteOpts::Defaults())
    );

    // >> Write table to stream (TODO: probably need a batch size)
    ARROW_RETURN_NOT_OK(batch_writer->WriteTable(*table));

    // >> Close the writer
    ARROW_RETURN_NOT_OK(batch_writer->Close());

    // >> Then; close, finish, and return the buffer
    if (not batch_bufstream->closed()) { batch_bufstream->Close(); }
    return batch_bufstream->Finish();
}
