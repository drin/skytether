#include "../headers/skytether.hpp"
#include "../headers/sql.hpp"
#include "../headers/operators.hpp"


extern void PrintPlan(QueryPlan query_plan);


int main(int argc, char **argv) {
    if (argc != 2) {
        printf("parse-query <SQL query>\n");
        return 1;
    }

    // Parse the input query
    std::string query_str(argv[1]);
    sky_debug_printf("Input query: '%s'\n", query_str.c_str());

    arrow::Result<QueryPlan> query_ast = parse_query(query_str.c_str());
    if (not query_ast.ok()) {
        sky_debug_printf(
             "Unable to parse input query:\n\t%s\n"
            ,query_ast.status().ToString().c_str()
        );
        return 1;
    }

    PrintPlan(query_ast.ValueOrDie());

    return 0;
}

