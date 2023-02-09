#include "../headers/skytether.hpp"

int main(int argc, char **argv) {
    if (argc != 2) {
        printf("explain <SQL query>\n");
        return 1;
    }

    std::string query_str(argv[1]);
    sky_debug_printf("Input query: '%s'\n", query_str.c_str());

    arrow::Result<QueryPlan> query_ast = parse_query(query_str.c_str());

    return 0;
}
