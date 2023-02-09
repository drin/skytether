/**
 * An arbitrary collection of Functions that are commonly useful for converting data types, etc.
 */

#include "../headers/skytether.hpp"


// ------------------------------
// Functions

// >> Type conversions

/*
template<typename ElementType, typename BuilderType>
arrow::Result<ArrayPtr>
VecToArrow(const std::vector<ElementType> &src_data) {
    BuilderType array_builder;
    ARROW_RETURN_NOT_OK(array_builder.Resize(src_data.size()));
    ARROW_RETURN_NOT_OK(array_builder.AppendValues(src_data));

    ArrayPtr vec_as_arrow;
    ARROW_RETURN_NOT_OK(array_builder.Finish(&vec_as_arrow));

    return arrow::Result<ArrayPtr>(vec_as_arrow);
}

template
arrow::Result<ArrayPtr>
VecToArrow<int32_t, arrow::Int32Builder>(const std::vector<int32_t> &src_data);
*/


// >> Type validations

arrow::Result<ChunkedArrayPtr>
UnionColumnByKey(TablePtr left_table, TablePtr right_table, int key_ndx) {
    DataTypePtr  left_coltype =  left_table->field(key_ndx)->type();
    DataTypePtr right_coltype = right_table->field(key_ndx)->type();

    if (not left_coltype->Equals(right_coltype)) {
        return arrow::Status::Invalid(
            "Dictionary merge requires input columns to be the same type"
        );
    }

    ArrayVec joined_colvec =  left_table->column(key_ndx)->chunks();
    ArrayVec right_colvec  = right_table->column(key_ndx)->chunks();
    joined_colvec.insert(joined_colvec.end(), right_colvec.begin(), right_colvec.end());

    // Create a shared pointer, then wrap it in a `Result`
    return arrow::Result<ChunkedArrayPtr>(
        std::make_shared<ChunkedArray>(std::move(joined_colvec), left_coltype)
    );
}
