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
