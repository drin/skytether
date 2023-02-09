#include "../headers/skytether.hpp"
#include "../headers/dataformats.hpp"

#if USE_BOOSTFS == 1
	namespace fs = boost::filesystem;
#else
	namespace fs = std::filesystem;
#endif

using arrow::compute::Expression;


arrow::Result<TablePtr>
execute_query(QueryPlan query_plan) {
    TableVec       result_tables;
    RecordBatchPtr parsed_recordbatch;

    // This for loop will eventually be driven by a metadata key value (the base key)
    for (size_t partition_ndx = 0; partition_ndx < 5; partition_ndx++) {

        // Construct a reader for the partition
        arrow::Result<BatchStreamRdrPtr> reader_result = ReaderForPartition(
            query_plan.source_table, partition_ndx
        );

        if (not reader_result.ok()) { break; }
        BatchStreamRdrPtr batch_reader = reader_result.ValueOrDie();

        // For each batch in the partition
        ARROW_RETURN_NOT_OK(batch_reader->ReadNext(&parsed_recordbatch));
        sky_debug_printf("Read record batch\n");

        while (parsed_recordbatch != nullptr) {
            ARROW_ASSIGN_OR_RAISE(
                  TablePtr result_table_batch
                 ,ScanTable(query_plan, parsed_recordbatch)
            );

            // Do a debug print for now of the result
            sky_debug_block(PrintTable(result_table_batch);)
            result_tables.push_back(result_table_batch);

            ARROW_RETURN_NOT_OK(batch_reader->ReadNext(&parsed_recordbatch));
        }
    }

    return arrow::ConcatenateTables(result_tables);
}


int main(int argc, char **argv) {
    if (argc != 3) {
        printf("exec-query <base output path> <SQL query>\n");
        return 1;
    }

    // prepare output path
    fs::path path_to_output_base(local_file_protocol + fs::absolute(argv[1]).string());

    // Parse the input query
    std::string query_str(argv[2]);
    sky_debug_printf("Input query: '%s'\n", query_str.c_str());
    arrow::Result<QueryPlan> query_plan = parse_query(query_str.c_str());
    if (not query_plan.ok()) {
        sky_debug_printf("Unable to parse input query\n");
        return 1;
    }

    arrow::Result<TablePtr> query_result = execute_query(query_plan.ValueOrDie());
    if (not query_result.ok()) {
        std::cerr << "Query execution failed" << std::endl;
        return 1;
    }

    // Checkout an excerpt of the table
    sky_debug_printf("Query result to be written:\n");
    sky_debug_block(PrintTable(query_result.ValueOrDie());)

    int64_t batch_size = 2048;
    arrow::Status write_status = WriteAsRecordBatches(
        query_result.ValueOrDie()   ,
        path_to_output_base.string(),
        batch_size
    );

    if (not write_status.ok()) {
        sky_debug_printf("Failed to write output files\n");
        return 1;
    }

    return 0;
}

