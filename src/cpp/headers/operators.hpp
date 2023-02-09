/**
 * Overall header for this library
 * Author: Aldrin Montana
 */

#ifndef __OPERATORS_HPP
#define __OPERATORS_HPP


// ------------------------------
// Dependencies

#include "skytether.hpp"
#include <unordered_map>


// ------------------------------
// Macros and aliases

// >> Arrow: Numeric compute functions
using arrow::compute::Add;
using arrow::compute::Subtract;
using arrow::compute::Divide;
using arrow::compute::Multiply;
using arrow::compute::Power;
using arrow::compute::AbsoluteValue;


// >> Other compute functions
using arrow::compute::DictionaryEncode;
using arrow::compute::IsIn;
using arrow::compute::Filter;
using arrow::compute::Take;

// >> Option types
using arrow::compute::SetLookupOptions;


// ------------------------------
// Classes

// >> Higher-level data structures

/**
 * This is simply an arrow `DictionaryArray` that is chunked (`ChunkedArray`). However, the Arrow
 * interfaces don't make it easy to access DictionaryArray interfaces which are wrapped by the
 * ChunkedArray interfaces, so we create this struct to make that easy for us. We also include
 * many convenience functions.
 */
struct ChunkedDict {
    shared_ptr<ChunkedArray> encoded_chunks;

    ~ChunkedDict();
    ChunkedDict();

    Status SetData(shared_ptr<ChunkedArray> table_column);

    // retrieves the unique words that the encodings represent
    shared_ptr<Array>      words();
    shared_ptr<Int64Array> encodings(int chunk_ndx);

    int64_t Length();
    int64_t WordCount();
    int     ChunkCount();

    // Search operations

    /** Retrieves the first occurrence of unique indices. */
    unique_ptr<Int64Vec> FirstIndices();

    /** Retrieves the last occurrence of unique indices. */
    unique_ptr<Int64Vec> LastIndices();

    /** Retrieves indices that have only 1 occurrence. */
    unique_ptr<Int64Vec> UniqueIndices();
};


// >> Aggregates
/* For reference, the t-statistic:
 *
 * ttest_num         <- (apply(sample_one, 1, mean) - apply(sample_two, 1, mean))
 * ttest_denom_covar <- (
 *     (
 *        (ncol(sample_one) - 1) * (apply(sample_one, 1, sd) ** 2)
 *      + (ncol(sample_two) - 1) * (apply(sample_two, 1, sd) ** 2)
 *     )
 *     / (ncol(sample_one) + ncol(sample_two) - 2)
 * )
 * ttest_denomright <- sqrt((1 / ncol(sample_one)) + (1 / ncol(sample_two)))
 * calculated_ttests <- (ttest_num / (sqrt(ttest_denom_covar) * ttest_denomright))
 */


struct MeanAggr {
    uint64_t                 count;
    shared_ptr<ChunkedArray> means;
    shared_ptr<ChunkedArray> variances;

    ~MeanAggr();
    MeanAggr();

    Status Initialize(shared_ptr<ChunkedArray> initial_vals);
    Status Accumulate(shared_ptr<Table> new_vals, int64_t col_ndx = 1);
    Status Combine(const MeanAggr *other_aggr);

    Status PrintM1M2();
    Status PrintState();

    shared_ptr<Table> TakeResult();
    shared_ptr<Table> ComputeTStatWith(const MeanAggr &other_aggr);
};


// ------------------------------
// API Functions

// >> General Operators
//    >    Merging
Result<shared_ptr<Table>>
HashMergeTakeLatest(shared_ptr<Table> left_table, shared_ptr<Table> right_table, int key_ndx);

//    > Selections
Result<shared_ptr<ChunkedArray>>
SelectPartitionsByCluster(shared_ptr<Table> cluster_map, const StrVec &cluster_names);

Result<shared_ptr<ChunkedArray>>
SelectCellsByCluster(shared_ptr<Table> cell_clusters, const StrVec &cluster_names);

Result<shared_ptr<Table>>
SelectTableByRow(shared_ptr<Table> input_tables, Int64Vec &row_indices);

//     > Projections
Result<shared_ptr<Schema>>
TakeSchemaColumns(shared_ptr<Schema> input_schema, shared_ptr<ChunkedArray> cell_indices);

Result<shared_ptr<Table>>
TakeTableColumns(shared_ptr<Table> input_table, shared_ptr<ChunkedArray> cell_indices);

//     > Join helpers
Result<shared_ptr<Table>>
ExtendTable(shared_ptr<Table> base_table, shared_ptr<Table> ext_table);

shared_ptr<RecordBatch>
CreateEmptyRecordBatch(shared_ptr<Schema> batch_schema, StrVec &row_ids);

Result<shared_ptr<Table>>
CopyMatchedRows(shared_ptr<ChunkedArray> ordered_ids, shared_ptr<Table> src_table);

// >> Query Processing
//    >    parsing
Result<QueryPlan> parse_query(const char *query_str);

//    >    projections
Result<shared_ptr<Table>>  ProjectByIndex(shared_ptr<Table> table_data, Int32Vec attr_indices);
Result<shared_ptr<Table>>  ProjectByName (shared_ptr<Table> table_data, StrVec   attr_names  );

//    >    selections
Result<shared_ptr<Table>>  ScanTable(QueryPlan query_plan, shared_ptr<RecordBatch> table_batch);

// >> Serialize and deserialize
Result<shared_ptr<Buffer>> WriteSchemaToBuffer(shared_ptr<Schema> schema);
Result<shared_ptr<Buffer>> WriteRecordBatchToBuffer(shared_ptr<Schema> schema, shared_ptr<RecordBatch> record_batch);
Result<shared_ptr<Buffer>> WriteTableToBuffer(shared_ptr<Table> table);

// >> Numeric operators
shared_ptr<ChunkedArray> VecAdd(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op);
shared_ptr<ChunkedArray> VecSub(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op);
shared_ptr<ChunkedArray> VecMul(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op);
shared_ptr<ChunkedArray> VecMul(shared_ptr<ChunkedArray> left_op, Datum                    right_op);

shared_ptr<ChunkedArray> VecDiv(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op);
shared_ptr<ChunkedArray> VecDiv(shared_ptr<ChunkedArray> left_op, Datum                    right_op);

shared_ptr<ChunkedArray> VecPow(shared_ptr<ChunkedArray> left_op, Datum                    right_op);

shared_ptr<ChunkedArray> VecAbs(shared_ptr<ChunkedArray> unary_op);


#endif // __OPERATORS_HPP
