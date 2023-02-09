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
 * Implementations for various "operators". This isn't very general right now, but eventually it
 * will hopefully look like relational operators, or things that are fairly re-usable.
 */


// ------------------------------
// Dependencies

#include "../headers/operators.hpp"

// NOTE: For debugging
#include <ctime>
#include <iomanip>


// ------------------------------
// Macros and aliases

using std::unordered_map;
using arrow::util::string_view;


// ------------------------------
// Functions

// >> Templated

template<typename ElementType, typename BuilderType>
Result<shared_ptr<Array>>
VecToArrow(const std::vector<ElementType> &src_data) {
    BuilderType array_builder;
    ARROW_RETURN_NOT_OK(array_builder.Resize(src_data.size()));
    ARROW_RETURN_NOT_OK(array_builder.AppendValues(src_data));

    shared_ptr<Array> vec_as_arrow;
    ARROW_RETURN_NOT_OK(array_builder.Finish(&vec_as_arrow));

    return Result<shared_ptr<Array>>(vec_as_arrow);
}


// >> ChunkedDict functions
ChunkedDict::~ChunkedDict() {
    encoded_chunks.reset();
}

ChunkedDict::ChunkedDict() {
    encoded_chunks = shared_ptr<ChunkedArray>(nullptr);
}

Status ChunkedDict::SetData(shared_ptr<ChunkedArray> table_column) {
    ARROW_ASSIGN_OR_RAISE(Datum encoded_chunkarr, DictionaryEncode(table_column));

    encoded_chunks = encoded_chunkarr.chunked_array();
    return Status::OK();
}

shared_ptr<Array> ChunkedDict::words() {
    // each chunk references the same "words" array
    auto dict_chunk = std::static_pointer_cast<DictionaryArray>(
        encoded_chunks->chunk(0)
    );

    return dict_chunk->dictionary();
}

int64_t ChunkedDict::Length()    { return encoded_chunks->length();     }
int64_t ChunkedDict::WordCount() { return this->words()->length();      }
int ChunkedDict::ChunkCount()    { return encoded_chunks->num_chunks(); }

shared_ptr<Int64Array> ChunkedDict::encodings(int chunk_ndx) {
    auto dict_chunk = std::static_pointer_cast<DictionaryArray>(
        encoded_chunks->chunk(chunk_ndx)
    );

    return std::static_pointer_cast<Int64Array>(dict_chunk->indices());
}

unique_ptr<Int64Vec> ChunkedDict::FirstIndices() {
    // For building the result
    int64_t  chunk_offset  = 0;
    int64_t  unique_count  = this->WordCount();
    unique_ptr<Int64Vec> first_indices = std::make_unique<Int64Vec>(unique_count, -1);

    // For semi-efficient iteration
    int64_t assigned_ids = 0;

    for (int chunk_ndx = 0; chunk_ndx < this->ChunkCount(); chunk_ndx++) {
        // short-circuit; here for readability
        if (assigned_ids >= unique_count) { break; }

        shared_ptr<Int64Array> encoded_ids = this->encodings(chunk_ndx);
        const int64_t *id_arr = encoded_ids->raw_values();

        // grab the encodings for each unique value
        int64_t chunk_size  = encoded_ids->length();
        for (int64_t chunk_subndx = 0; chunk_subndx < chunk_size; chunk_subndx++) {
            // short-circuit; here for readability
            if (assigned_ids >= unique_count) { break; }

            // keep only the highest index of an encoded ID
            int64_t encoded_id = id_arr[chunk_subndx];
            if ((*first_indices)[encoded_id] < 0) {
                (*first_indices)[encoded_id] = chunk_offset + chunk_subndx;
                assigned_ids++;
            }
        }

        chunk_offset += chunk_size;
    }

    return first_indices;
}

unique_ptr<Int64Vec> ChunkedDict::LastIndices() {
    // For building the result
    int64_t chunk_offset = 0;
    auto    unique_count = this->WordCount();
    auto    dict_size    = this->Length();
    auto    last_indices = std::make_unique<Int64Vec>(unique_count, -1);

    // For semi-efficient iteration
    int64_t assigned_ids = 0;

    for (auto chunk_ndx = this->ChunkCount(); chunk_ndx > 0; chunk_ndx--) {
        // short-circuit; here for readability
        if (assigned_ids >= unique_count) { break; }

        auto encoded_ids      = this->encodings(chunk_ndx - 1);
        auto chunk_size       = encoded_ids->length();
        const int64_t *id_arr = encoded_ids->raw_values();

        // grab the encodings for each unique value
        for (auto chunk_subndx = chunk_size; chunk_subndx > 0; chunk_subndx--) {
            // short-circuit; here for readability
            if (assigned_ids >= unique_count) { break; }

            // keep only the highest index of an encoded ID
            auto encoded_id = id_arr[chunk_subndx - 1];
            if ((*last_indices)[encoded_id] < 0) {
                auto table_rowndx = (
                      dict_size
                    - chunk_offset
                    - (chunk_size - chunk_subndx)
                );

                (*last_indices)[encoded_id] = table_rowndx - 1;
                assigned_ids++;
            }
        }

        chunk_offset += chunk_size;
    }

    return last_indices;
}


unique_ptr<Int64Vec> ChunkedDict::UniqueIndices() {
    int64_t chunk_offset   = 0;
    auto    unique_indices = std::make_unique<Int64Vec>(this->WordCount(), -1);

    for (int chunk_ndx = 0; chunk_ndx < this->ChunkCount(); chunk_ndx++) {
        auto encoded_ids      = this->encodings(chunk_ndx);
        auto chunk_size       = encoded_ids->length();
        const int64_t *id_arr = encoded_ids->raw_values();

        // grab the encodings for each unique value
        for (int64_t chunk_subndx = 0; chunk_subndx < chunk_size; chunk_subndx++) {
            auto encoded_id = id_arr[chunk_subndx];

            if ((*unique_indices)[encoded_id] < 0) {
                (*unique_indices)[encoded_id] = chunk_offset + chunk_subndx;
            }
            else if ((*unique_indices)[encoded_id] > 0) {
                (*unique_indices)[encoded_id] = 0;
            }
        }

        chunk_offset += chunk_size;
    }

    // remove all indices for non-unique values
    for (auto unique_itr = unique_indices->begin(); unique_itr != unique_indices->end();) {
        if (*unique_itr <= 0) { unique_itr  = unique_indices->erase(unique_itr); }
        else                  { unique_itr += 1;                                 }
    }

    return unique_indices;
}


Result<shared_ptr<ChunkedArray>>
SelectPartitionsByCluster(shared_ptr<Table> cluster_map, const StrVec &cluster_names) {
    // hard-coded values that can be parameterized later
    bool drop_nulls   = true;    // Arrow option: whether we consider null values or not
    int  colndx_cname = 0;       // index of cluster   name
    int  colndx_pname = 1;       // index of partition name

    // convert vector<string> to Array<arrow.string()>
    auto result = VecToArrow<std::string, arrow::StringBuilder>(cluster_names);
    if (not result.ok()) { return Result<shared_ptr<ChunkedArray>>(result.status()); }

    // Apply `IsIn` logical function, that is just set-based intersection
    ARROW_ASSIGN_OR_RAISE(
         Datum row_ismatch
        ,IsIn(
              cluster_map->column(colndx_cname)
             ,SetLookupOptions { result.ValueOrDie(), drop_nulls }
         )
    );

    // Apply `Filter` to input table; keep the entries that intersect `cluster_names`
    ARROW_ASSIGN_OR_RAISE(Datum filter_result, Filter(cluster_map, row_ismatch));

    // Return selection of partition name column
    return filter_result.table()->column(colndx_pname);
}

/*
 * A facade for now. Will maybe rename and deprecate this function when I can.
 */
Result<shared_ptr<ChunkedArray>>
SelectCellsByCluster(shared_ptr<Table> cell_clusters, const StrVec &cluster_names) {
    // TODO: assumes cluster names are in 1st column and cell indices in 2nd
    return SelectPartitionsByCluster(cell_clusters, cluster_names);
}


// TODO: eventually decompose this so that each chunk can be individually updated
Result<shared_ptr<Table>>
SelectTableByRow(shared_ptr<Table> input_table, Int64Vec &row_indices) {
    // Add indices of encodedIDs to selection indices
    auto convert_result = VecToArrow<int64_t, arrow::Int64Builder>(row_indices);
    if (not convert_result.ok()) {
        return arrow::Status::Invalid("Could not convert vector to array");
    }

    ARROW_ASSIGN_OR_RAISE(
         Datum selection_result
        ,Take(input_table, convert_result.ValueOrDie())
    );

    return Result<shared_ptr<Table>>(
        arrow::util::get<shared_ptr<Table>>(selection_result.value)
    );
}

// >> Projections

Result<shared_ptr<Schema>>
TakeSchemaColumns(shared_ptr<Schema> input_schema, shared_ptr<ChunkedArray> cell_indices) {
    auto selected_fields = FieldVec(cell_indices->length());
    size_t selection_ndx = 0;

    // for each chunk
    for (int chunk_ndx = 0; chunk_ndx < cell_indices->num_chunks(); ++chunk_ndx) {
        auto           chunk_data  = std::static_pointer_cast<Int32Array>(cell_indices->chunk(chunk_ndx));
        const int32_t *chunk_vals = chunk_data->raw_values();

        // for each element in a chunk
        for (int chunk_subndx = 0; chunk_subndx < chunk_data->length(); ++chunk_subndx) {

            // grab the schema field
            selected_fields[selection_ndx++] = input_schema->field(chunk_vals[chunk_subndx]);
        }
    }

    return std::make_shared<Schema>(selected_fields)->WithMetadata(input_schema->metadata());
}


Result<shared_ptr<Table>>
TakeTableColumns(shared_ptr<Table> input_table, shared_ptr<ChunkedArray> cell_indices) {
    // length + 1 because cell_indices *should not* refer to the first column (gene name)
    // so, we hard-code it for now
    auto col_indices  = std::make_unique<Int32Vec>(cell_indices->length() + 1, -1);
    size_t index_ndx  = 0;

    // TODO: fix this hacky-ness
    // Always take the first column, assuming it's row names (as in dataframes)
    (*col_indices)[index_ndx] = 0;

    // build all of the indices first, so that we can make a single call to `SelectColumns`
    for (int chunk_ndx = 0; chunk_ndx < cell_indices->num_chunks(); ++chunk_ndx) {
        auto chunk_data = std::static_pointer_cast<Int32Array>(cell_indices->chunk(chunk_ndx));

        const int32_t *chunk_vals = chunk_data->raw_values();
        for (int chunk_subndx = 0; chunk_subndx < chunk_data->length(); ++chunk_subndx) {
            (*col_indices)[++index_ndx] = chunk_vals[chunk_subndx];
        }
    }

    return input_table->SelectColumns(*col_indices);
}


// >> Join helpers

// NOTE: This function assumes the first column of both tables are identical.
//       More specifically, this is a naive join in the simplest scenario.
Result<shared_ptr<Table>>
ExtendTable(shared_ptr<Table> base_table, shared_ptr<Table> ext_table) {
    sky_debug_printf("[ExtendTable] Extending schema fields and table columns\n");

    // merge schema metadata from both tables
    auto ext_schema       = ext_table->schema();
    auto base_schema      = base_table->schema();
    auto base_schema_meta = base_schema->metadata()->Copy();
    auto combined_meta    = base_schema_meta->Merge(*ext_schema->metadata());

    // Concatenate schema fields
    FieldVec combined_fields { base_schema->fields() };
    combined_fields.insert(
         combined_fields.end()
         // do this if we assume the first col is identical
         // ,++ext_schema->fields().cbegin()
        ,ext_schema->fields().cbegin()
        ,ext_schema->fields().cend()
    );

    // Concatenate table columns
    ChunkedArrVec combined_cols { base_table->columns() };
    combined_cols.insert(
         combined_cols.end()
         // do this if we assume the first col is identical
         // ,++ext_table->columns().cbegin()
        ,ext_table->columns().cbegin()
        ,ext_table->columns().cend()
    );

    // Construct new table from concatenated schema fields and table columns
    return Table::Make(arrow::schema(combined_fields, combined_meta), combined_cols);
}

// NOTE: this uses string_view, because CopyMatchedRows does. Maybe figure out how to generalize
// this later
shared_ptr<RecordBatch>
CreateEmptyRecordBatch(shared_ptr<Schema> batch_schema, StrVec &row_ids) {
    // can be parameterized later; if desired
    double              default_fillval = 0;
    ArrayVec            batch_data;
    std::vector<double> fill_values(row_ids.size(), default_fillval);

    // First, add the IDs (assumed to be gene IDs for now)
    auto rowid_result = VecToArrow<std::string, arrow::StringBuilder>(row_ids);
    if (not rowid_result.ok()) {
        std::cerr << "Unable to set IDs for empty RecordBatch" << std::endl;
        return nullptr;
    }

    batch_data.emplace_back(rowid_result.ValueOrDie());

    // Then, fill all of the <empty> values
    for (int col_ndx = 1; col_ndx < batch_schema->num_fields(); ++col_ndx) {
        auto fill_result = VecToArrow<double, arrow::DoubleBuilder>(fill_values);
        if (not fill_result.ok()) {
            std::cerr << "Unable to fill column for empty RecordBatch"
                      << " [" << col_ndx << "]"
                      << std::endl
            ;

            return nullptr;
        }

        batch_data.emplace_back(fill_result.ValueOrDie());
    }

    return RecordBatch::Make(batch_schema, row_ids.size(), batch_data);
}


Result<shared_ptr<Table>>
CopyMatchedRows(shared_ptr<ChunkedArray> ordered_ids, shared_ptr<Table> src_table) {
    // hash primary keys (gene IDs) in src_table to match against later
    int64_t src_rowndx = 0;
    auto    src_keys   = src_table->column(0);

    unordered_map<string_view, int64_t> src_keymap;
    src_keymap.reserve((size_t) src_keys->length());

    // >> TODO: get start time here
    for (int chunk_ndx = 0; chunk_ndx < src_keys->num_chunks(); ++chunk_ndx) {
        shared_ptr<StringArray> chunk_srckeys = std::static_pointer_cast<StringArray>(
            src_keys->chunk(chunk_ndx)
        );

        for (const auto src_key : *chunk_srckeys) {
            if (not src_key.has_value()) { continue; }

            // add a mapping of this value (gene ID) to it's row index
            src_keymap.insert({ src_key.value(), src_rowndx });
            ++src_rowndx;
        }
    }

    // Construct a vector of RecordBatches to convert into a result table
    StrVec         missing_ids;
    Int64Vec       match_indices;
    RecordBatchVec result_batches;

    // >> TODO: interval time: source keymap <--> populate missing rows
    for (int chunk_ndx = 0; chunk_ndx < ordered_ids->num_chunks(); ++chunk_ndx) {
        shared_ptr<StringArray> chunk_srckeys = std::static_pointer_cast<StringArray>(
            ordered_ids->chunk(chunk_ndx)
        );

        // time stamp each iteration: find variability in processing each chunk
        auto timestamp = std::time(nullptr);

        /* NOTE: put_time comes from <iomanip>
        std::cout << "[" << std::put_time(std::localtime(&timestamp), "%T") << "] "
                  << "chunk: " << chunk_ndx
                  << std::endl
        ;
        */

        for (const auto src_key : *chunk_srckeys) {
            auto src_keyval = src_key.value_or("");

            // If the source value is found, add the matched index
            if (src_key.has_value() and src_keymap.count(src_keyval)) {
                // Since we have a match, drain all mismatches
                if (missing_ids.size() > 0) {
                    result_batches.push_back(
                        CreateEmptyRecordBatch(src_table->schema(), missing_ids)
                    );

                    missing_ids.clear();
                }

                match_indices.push_back(src_keymap[src_keyval]);

                // use `continue` to reduce code indentation for the "else" case
                continue;
            }

            // Otherwise, drain matches by appending `take` selection as RecordBatches
            if (match_indices.size() > 0) {
                ARROW_ASSIGN_OR_RAISE(
                     auto table_selection
                    ,SelectTableByRow(src_table, match_indices)
                )

                // push RecordBatches onto the end of result_batches
                TableBatchReader batch_converter(*table_selection);
                ARROW_RETURN_NOT_OK(batch_converter.ReadAll(&result_batches));

                match_indices.clear();
            }

            // if we're here, then the ID didn't have a hit in src_keymap
            missing_ids.push_back(std::string { src_keyval });
        }
    }

    // std::cout << std::endl;

    // drain any remaining missing IDs
    if (missing_ids.size() > 0) {
        result_batches.push_back(
            CreateEmptyRecordBatch(src_table->schema(), missing_ids)
        );

        missing_ids.clear();
    }

    // drain any remaining matched IDs
    if (match_indices.size() > 0) {
        ARROW_ASSIGN_OR_RAISE(
             auto table_selection
            ,SelectTableByRow(src_table, match_indices)
        )

        // push RecordBatches onto the end of result_batches
        TableBatchReader batch_converter(*table_selection);
        ARROW_RETURN_NOT_OK(batch_converter.ReadAll(&result_batches));

        match_indices.clear();
    }

    // >> TODO: get finish time for copying source rows into dense table

    return Table::FromRecordBatches(src_table->schema(), result_batches);
}


// >> Mergers (join, but for KV instead of sets)
Result<shared_ptr<Table>>
HashMergeTakeLatest(shared_ptr<Table> left_table, shared_ptr<Table> right_table, int key_ndx) {
    // union the tables
    ARROW_ASSIGN_OR_RAISE(
         auto table_union
        ,arrow::ConcatenateTables({ left_table, right_table })
    );

    // dictionary encode the union to find unique values in the specified column
    ChunkedDict pkey_index {};
    pkey_index.SetData(table_union->column(key_ndx));

    // return the largest index for each unique value
    return SelectTableByRow(table_union, *(pkey_index.LastIndices()));
}

