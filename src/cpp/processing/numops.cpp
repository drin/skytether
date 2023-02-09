/**
 * Implementations for various "operators". This isn't very general right now, but eventually it
 * will hopefully look like relational operators, or things that are fairly re-usable.
 *
 * Author: Aldrin Montana
 */


// ------------------------------
// Dependencies

#include "../headers/operators.hpp"


// ------------------------------
// Functions

shared_ptr<ChunkedArray>
VecAdd(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Add(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecSub(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Subtract(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecDiv(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Divide(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecDiv(shared_ptr<ChunkedArray> left_op, Datum right_op) {
    if (left_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Divide(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecMul(shared_ptr<ChunkedArray> left_op, shared_ptr<ChunkedArray> right_op) {
    if (left_op == nullptr or right_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Multiply(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecMul(shared_ptr<ChunkedArray> left_op, Datum right_op) {
    if (left_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Multiply(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecPow(shared_ptr<ChunkedArray> left_op, Datum right_op) {
    if (left_op == nullptr) { return nullptr; }

    Result<Datum> op_result = Power(left_op, right_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}


shared_ptr<ChunkedArray>
VecAbs(shared_ptr<ChunkedArray> unary_op) {
    if (unary_op == nullptr) { return nullptr; }

    Result<Datum> op_result = AbsoluteValue(unary_op);
    if (not op_result.ok()) { return nullptr; }

    return std::make_shared<ChunkedArray>(
        std::move(op_result.ValueOrDie()).chunks()
    );
}
