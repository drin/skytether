#include <fstream>


std::vector<std::string>
normalize_file_record(std::string record_as_str, const char delim) {
    // const char *record_str_at_pos = record_as_str.c_str();

    std::vector<std::string> normalized_record;
    size_t field_startpos = -1;

    size_t ndx;
    for (ndx = 0; ndx < record_as_str.length(); ndx++) {
        if (record_as_str.at(ndx) == delim) {
            if (field_startpos != -1) {
                // zero-copy substring with range: [startpos, endpos)
                normalized_record.push_back(
                    std::string(record_as_str, field_startpos, ndx - field_startpos)
                );

                // record is parsed or reset for next field
                if (normalized_record.size() == attribute_names.size()) { break; }
                else { field_startpos = -1; }
            }

            continue;
        }

        // skip non-desirable characters and set the start position when appropriate
        switch (record_as_str.at(ndx)) {
            // prefix/suffix characters we want to skip
            case ' ':
            case '\t':
            case '\n':
            case '"':
            case '\'':
                break;

            // if this is a good character, make sure we set start position
            default:
                if (field_startpos == -1) { field_startpos = ndx; }
                break;
        }
    }

    if (field_startpos != -1) {
        // zero-copy substring with range: [startpos, endpos)
        normalized_record.push_back(
            std::string(record_as_str, field_startpos, ndx - field_startpos)
        );
    }

    return normalized_record;
}


std::vector<std::string>
normalize_tsv_record(std::string record_as_str) {
    return normalize_file_record(record_as_str, '\t');
}


std::vector<std::vector<std::string>>
parse_tsv_file(const char *path_to_file) {
    std::string   file_line;
    std::ifstream input_file (path_to_file);

    std::vector<std::vector<std::string>> parsed_filedata;

    while (input_file.is_open() and not input_file.eof()) {
        std::getline(input_file, file_line);
        #ifdef DEBUG 
            std::cout << "Parsed line: '" << file_line << "'" << std::endl;
        #endif


        std::vector<std::string> parsed_record = normalize_tsv_record(file_line);
        if (not parsed_record.size()) { continue; }

        parsed_filedata.push_back(parsed_record);
    }

    input_file.close();

    return parsed_filedata;
}


int main(int argc, char **argv) {
    if (argc != 2) {
        std::cout << "Please provide just the path to a file to parse" << std::endl;
        return 1;
    }

    // std::cout << "Parsing file: '" << argv[1] << "'" << std::endl;
    std::vector<std::vector<std::string>> file_data = parse_tsv_file(argv[1]);

    print_filedata(file_data);

    return 0;
}
