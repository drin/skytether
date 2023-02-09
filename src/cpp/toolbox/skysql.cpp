
// ------------------------------
// Dependencies

#include "../headers/skytether.hpp"
#include "../headers/skykinetic.hpp"
#include "../headers/operators.hpp"


// ------------------------------
// Macros and aliases

// variables
using Skytether::SLICE_DELIM;


// ------------------------------
// Functions

Result<shared_ptr<Table>>
execute_query(KineticConn &kconn, QueryPlan query_plan) {
    TableVec result_tables;

    ARROW_ASSIGN_OR_RAISE(auto pmeta, kconn.GetPartitionMeta(query_plan.source_table));
    ARROW_ASSIGN_OR_RAISE(auto slice_count, pmeta->GetPartitionCount());
    ARROW_ASSIGN_OR_RAISE(auto stripe_size, pmeta->GetStripeSize());

    // for each slice in the partition
    for (size_t slice_ndx = 0; slice_ndx < slice_count; slice_ndx++) {
        auto pslice = std::make_shared<PartitionSlice>(query_plan.source_table, slice_ndx);
        ARROW_RETURN_NOT_OK(kconn.GetPartitionSlice(pslice, stripe_size));

        // For each batch in the slice (should be just 1)
        shared_ptr<RecordBatch> parsed_recordbatch;
        auto table_reader = TableBatchReader(*(pslice->data));
        auto read_status  = table_reader.ReadNext(&parsed_recordbatch);

        while (read_status.ok() and parsed_recordbatch != nullptr) {
            ARROW_ASSIGN_OR_RAISE(auto result_table_batch, ScanTable(query_plan, parsed_recordbatch));
            result_tables.push_back(result_table_batch);

            read_status = table_reader.ReadNext(&parsed_recordbatch);
        }

        if (not read_status.ok()) {
            std::cerr << "Failed to read next RecordBatch:" << std::endl
                      << "\t" << read_status.message()      << std::endl
            ;
            return Status::Invalid("Failed to read RecordBatch from partition slice");
        }
    }

    ARROW_ASSIGN_OR_RAISE(auto combined_result, arrow::ConcatenateTables(result_tables));

    return combined_result->CombineChunks();
}


int main(int argc, char **argv) {
    if (argc != 3) {
        printf("skysql <output keyspace (key prefix)> <SQL query>\n");
        return 1;
    }

    std::string result_prefix { argv[1] };
    std::string query_str     { argv[2] };

    // Parse the input query
    sky_debug_printf("Input query: '%s'\n", query_str.data());
    auto query_plan = parse_query(query_str.data());
    if (not query_plan.ok()) {
        sky_debug_printf("Unable to parse input query\n");
        return 1;
    }

    // Connect to kinetic drive
    KineticConn kconn;
    kconn.Connect();

    if (not kconn.IsConnected()) {
        sky_debug_printf("Unable to connect to kinetic server\n");
        return 1;
    }

    sky_debug_printf("Connection ID: %d\n", kconn.conn_id);

    // >> Execute query (read from drive and apply query)
    // Result<shared_ptr<Table>> query_result = kconn.ArrowReader(query_plan.source_table);
    auto query_result = execute_query(kconn, query_plan.ValueOrDie());
    if (not query_result.ok()) {
        std::cerr << "Query execution failed" << std::endl;
        kconn.Disconnect();
        return 1;
    }

    // Checkout an excerpt of the table
    sky_debug_printf("Query result to be written:\n");
    sky_debug_block(PrintTable(query_result.ValueOrDie());)

    // >> Persist results of query (write data to drive)
    sky_debug_printf("Persisting query results:\n");
    int64_t batch_size = 20480;

    auto put_status = kconn.PutPartition(result_prefix, query_result.ValueOrDie(), batch_size);
    if (not put_status.ok()) {
        sky_debug_printf("Failed to insert results into kinetic drive\n");
        kconn.Disconnect();
        return 1;
    }

    kconn.Disconnect();
    return 0;
}

