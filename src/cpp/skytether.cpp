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


#include "headers/skytether.hpp"

// >> KBuffer
KBuffer::~KBuffer() {
    Release();
}

KBuffer::KBuffer() {
    base = nullptr;
    len  = 0;
}

KBuffer::KBuffer(uint8_t *buf_base, size_t buf_len) {
    base = buf_base;
    len  = buf_len;
}

void KBuffer::Release() {
    // free(base);
    base = nullptr;
}

std::string KBuffer::ToString() {
    return std::string((char *) base, len);
}

bool KBuffer::Matches(char *other) {
    if (other == nullptr) { return false; }

    return 0 == strncmp((char *) base, other, len);
}

bool KBuffer::Matches(std::string token) {
    return token.compare(std::string((char *) base, len)) == 0;
}

bool KBuffer::Matches(KBuffer *other) {
    if (other == nullptr or len != other->len) { return false; }

    return 0 == strncmp((char *) base, (char *) other->base, len);
}

Result<shared_ptr<RecordBatchStreamReader>>
KBuffer::NewRecordBatchReader() {
    return RecordBatchStreamReader::Open(
         std::make_shared<BufferReader>(base, (int64_t) len)
        ,IPCReadOpts::Defaults()
    );
}


// >> Util functions

Result<shared_ptr<Array>> EmptyDoubles(int64_t val_count) {
    arrow::DoubleBuilder arr_builder;

    ARROW_RETURN_NOT_OK(arr_builder.AppendEmptyValues(val_count));

    return arr_builder.Finish();
}

Result<shared_ptr<Array>> CopyStrArray(shared_ptr<Array> src_array) {
    auto str_array = std::static_pointer_cast<StringArray>(src_array);

    arrow::StringBuilder array_builder;
    ARROW_RETURN_NOT_OK(array_builder.Resize(str_array->length()));

    for (const auto str_val_opt : *str_array) {
        array_builder.Append(str_val_opt.value_or(""));
    }

    return array_builder.Finish();
}


Result<shared_ptr<ScannerBuilder>>
ScannerForBatches(shared_ptr<Schema> data_schema, RecordBatchVec record_batches) {
    // Create an in-memory dataset to construct a ScannerBuilder from
    auto dataset = std::make_shared<InMemoryDataset>(data_schema, record_batches);

    // Create a ScannerBuilder to provide expressions to
    return dataset->NewScan();
}


Result<shared_ptr<ScannerBuilder>>
ScannerForBatch(shared_ptr<Schema> data_schema, shared_ptr<RecordBatch> record_batch) {
    return ScannerForBatches(data_schema, RecordBatchVec({ record_batch }));
}

void
PrintStrVec(StrVec record_data) {
    for (size_t field_ndx = 0; field_ndx < record_data.size(); field_ndx++) {
        std::cout << "\t | " << record_data.at(field_ndx) << " |";
    }
}


void
PrintStrTable(StrTable filedata) {
    int row_count = 5;

    #if DEBUG == 1
        row_count = 10;
    #elif DEBUG == 2
        row_count = filedata.size();
    #endif

    for (int row_ndx = 0; row_ndx < row_count; row_ndx++) {
        std::cout << row_ndx << " >>";

        PrintStrVec(filedata.at(row_ndx));

        std::cout << std::endl;
    }
}


void
PrintSchemaAttributes(shared_ptr<Schema> schema, int64_t offset, int64_t length) {
    bool    show_field_meta = true;
    int64_t field_count     = schema->num_fields();
    int64_t max_fieldndx    = field_count;

    std::cout << "Schema Excerpt ";

    if (length > 0) {
        max_fieldndx = length < field_count ? length : field_count;
        std::cout << "(" << max_fieldndx << " of " << field_count << ")";
    }

    else {
        std::cout << "(" << field_count << " of " << field_count << ")";
    }

    std::cout << std::endl << "-------------------------" << std::endl;

    for (int field_ndx = offset; field_ndx < max_fieldndx; field_ndx++) {
        shared_ptr<Field> schema_field = schema->field(field_ndx);
        std::cout << "\t[" << field_ndx << "]:" << std::endl;
        std::cout << "\t\t"
                  << schema_field->ToString(show_field_meta)
                  << std::endl
        ;
    }
}


void
PrintSchemaMetadata(shared_ptr<KVMetadata> schema_meta, int64_t offset, int64_t length) {
    // grab a reference to the metadata for convenience
    int64_t metakey_count = schema_meta->size();
    int64_t max_keyndx    = metakey_count;

    std::cout << "Schema Metadata excerpt ";

    if (length > 0) {
        max_keyndx = length < metakey_count ? length : metakey_count;
        std::cout << "(" << max_keyndx << " of " << metakey_count << ")";
    }

    else {
        std::cout << "(" << metakey_count << " of " << metakey_count << ")";
    }

    std::cout << std::endl << "------------------------" << std::endl;

    // skytether specific metadata
    Result<size_t> pcount_result = Skytether::GetPartitionCount(schema_meta);
    if (pcount_result.ok()) {
        std::cout << "\tdecoded partition count: " << std::to_string(*pcount_result)
                  << std::endl
        ;
    }
    else {
        std::cerr << "\tcould not decode partition count." << std::endl;
    }

    Result<uint8_t> ssize_result = Skytether::GetStripeSize(schema_meta);
    if (ssize_result.ok()) {
        std::cout << "\tdecoded stripe size: " << std::to_string(*ssize_result)
                  << std::endl
        ;
    }

    // any other metadata
    for (int64_t meta_ndx = offset; meta_ndx < max_keyndx; meta_ndx++) {
        std::cout << "\t[" << meta_ndx << "] "
                  << schema_meta->key(meta_ndx)
                  << " -> "
                  << schema_meta->value(meta_ndx)
                  << std::endl
        ;
    }
}


void
PrintSchema(shared_ptr<Schema> schema, int64_t offset, int64_t length) {
    std::cout << "Schema:" << std::endl;

    // >> Print some attributes (columns)
    PrintSchemaAttributes(schema, offset, length);

    // >> Print some metadata key-values (if there are any)
    if (schema->HasMetadata()) {
        PrintSchemaMetadata(schema->metadata()->Copy(), offset, length);
    }
}


void
PrintTable(shared_ptr<Table> table_data, int64_t offset, int64_t length) {
    shared_ptr<Table> table_slice;
    int64_t  row_count = table_data->num_rows();

    std::cout << "Table Excerpt ";

    if (length > 0) {
        int64_t max_rowndx = length < row_count ? length : row_count;
        table_slice = table_data->Slice(offset, max_rowndx);
        std::cout << "(" << max_rowndx << " of " << row_count << ")";
    }

    else {
        table_slice = table_data->Slice(offset);
        std::cout << "(" << row_count << " of " << row_count << ")";
    }

    std::cout << std::endl
              << "--------------" << std::endl
              << table_slice->ToString()
              << std::endl
    ;
}


void
PrintBatch(shared_ptr<RecordBatch> batch_data, int64_t offset, int64_t length) {
    shared_ptr<RecordBatch> batch_slice;
    int64_t        row_count = batch_data->num_rows();

    std::cout << "Table Excerpt ";

    if (length > 0) {
        batch_slice = batch_data->Slice(offset, length);
        std::cout << "(" << length << " of " << row_count << " rows)";
    }

    else {
        batch_slice = batch_data->Slice(offset);
        std::cout << "(" << row_count << " of " << row_count << " rows)";
    }

    std::cout << std::endl
              << "--------------" << std::endl
              << batch_slice->ToString()
              << std::endl
    ;
}


void
PrintTable(shared_ptr<Skytether::Partition> pdata, int64_t offset, int64_t length) {
    auto concat_result = pdata->AsTable();
    if (not concat_result.ok()) {
        std::cerr << "Failed to concat partition slices" << std::endl;
    }
    auto table_data = concat_result.ValueOrDie();

    shared_ptr<Table> table_slice;
    int64_t  row_count = table_data->num_rows();

    std::cout << "Table Excerpt ";

    if (length > 0) {
        int64_t max_rowndx = length < row_count ? length : row_count;
        table_slice = table_data->Slice(offset, max_rowndx);
        std::cout << "(" << max_rowndx << " of " << row_count << ")";
    }

    else {
        table_slice = table_data->Slice(offset);
        std::cout << "(" << row_count << " of " << row_count << ")";
    }

    std::cout << std::endl
              << "--------------" << std::endl
              << table_slice->ToString()
              << std::endl
    ;
}


// ------------------------------
// Code for datamodel

/*
Partition::SliceIteratorSeq&
Partition::SliceIteratorSeq::operator++() {
    // TODO
    return *this;
}
*/
