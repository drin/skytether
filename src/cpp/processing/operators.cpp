/**
 * Implementations for various "operators". This isn't very general right now, but eventually it
 * will hopefully look like relational operators, or things that are fairly re-usable.
 *
 * Author: Aldrin Montana
 */


// ------------------------------
// Dependencies

#include "../headers/operators.hpp"

// NOTE: For debugging
#include <ctime>
#include <iomanip>


// ------------------------------
// Functions for `Projection` operator

Result<shared_ptr<Table>>
ProjectByIndex(shared_ptr<Table> table_data, Int32Vec attr_indices) {
    return table_data->SelectColumns(attr_indices);
}

Result<shared_ptr<Table>>
ProjectByName(shared_ptr<Table> table_data, StrVec attr_names) {
    Int32Vec attr_indices;

    shared_ptr<Schema> table_schema = table_data->schema();
    for (const std::string &attr_name : attr_names) {
        auto attr_ndx = table_schema->GetFieldIndex(attr_name);

        // GetFieldIndex return -1 if not found
        if (attr_ndx >= 0) { attr_indices.push_back(attr_ndx); }
    }

    return ProjectByIndex(table_data, attr_indices);
}


// ------------------------------
// Functions for `Selection` operator

Result<shared_ptr<Table>>
ScanTable(QueryPlan query_plan, shared_ptr<RecordBatch> table_batch) {
    sky_debug_printf("Using query to scan table\n");

    // Create an in-memory dataset from the parsed record batch
    auto batch_as_dataset = std::make_shared<InMemoryDataset>(
         table_batch->schema()
        ,RecordBatchVec({ table_batch })
    );

    // Create a scanner to pass the expression
    ARROW_ASSIGN_OR_RAISE(auto scanbuilder, batch_as_dataset->NewScan());

    // Bind the projection columns and predicate
    // NOTE: this is a temporary solution for handling `*`
    StrVec projection_attrs;
    for (auto projection_attr : query_plan.attributes) {
        if (projection_attr.compare("*") != 0) {
            projection_attrs.push_back(projection_attr);
        }
        else {
            for (auto table_attr : table_batch->schema()->field_names()) {
                projection_attrs.push_back(table_attr);
            }
        }
    }

    if (projection_attrs.size()) {
        ARROW_RETURN_NOT_OK(scanbuilder->Project(projection_attrs));
    }

    if (query_plan.has_filter) {
        ARROW_RETURN_NOT_OK(scanbuilder->Filter(query_plan.filter));
    }

    // Then complete the scanner and return the result
    ARROW_ASSIGN_OR_RAISE(auto batch_scanner, scanbuilder->Finish());

    return batch_scanner->ToTable();
}
