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


// ------------------------------
// Functions

MeanAggr::~MeanAggr() {
    means.reset();
    variances.reset();
}

MeanAggr::MeanAggr() {
    count     = 0;
    means     = std::shared_ptr<ChunkedArray>(nullptr);
    variances = std::shared_ptr<ChunkedArray>(nullptr);
}

Status
MeanAggr::Initialize(shared_ptr<ChunkedArray> initial_vals) {
    arrow::DoubleBuilder initial_vars;

    ARROW_RETURN_NOT_OK(initial_vars.AppendEmptyValues(initial_vals->length()));
    ARROW_ASSIGN_OR_RAISE(shared_ptr<Array> built_arr, initial_vars.Finish());

    variances   = std::make_shared<ChunkedArray>(built_arr);
    means       = initial_vals;
    this->count = 1;

    return Status::OK();
}

shared_ptr<Table>
MeanAggr::TakeResult() {
    auto result_schema = arrow::schema({
          arrow::field("mean"    , arrow::float64())
         ,arrow::field("variance", arrow::float64())
     });

    return Table::Make(result_schema, { this->means, this->variances });
}

shared_ptr<Table>
MeanAggr::ComputeTStatWith(const MeanAggr &other_aggr) {
    auto left_size  =  this->count;
    auto right_size = other_aggr.count;

    auto  left_vars = VecDiv(this->variances     , Datum(left_size) );
    auto right_vars = VecDiv(other_aggr.variances, Datum(right_size));

    // Subtract means; numerator
    auto ttest_num = VecSub(this->means, other_aggr.means);

    // square root of scaling factor (sample sizes); right side of denom
    double scale_factor = std::sqrt((1.0 / left_size) + (1.0 / right_size));

    // And now the "complex" variance calculations
    auto ttest_denom = VecAdd(
         VecMul(left_vars , Datum( left_size - 1))
        ,VecMul(right_vars, Datum(right_size - 1))
    );

    ttest_denom = VecDiv(ttest_denom, Datum((left_size + right_size - 2)));
    ttest_denom = VecPow(ttest_denom, Datum(0.5));
    ttest_denom = VecMul(ttest_denom, Datum(scale_factor));

    return Table::Make(
         arrow::schema({ arrow::field("t-statistic", arrow::float64()) })
        ,{ VecDiv(ttest_num, ttest_denom) }
    );
}


/**
  * Per Wikipedipa; Moment2 (variance):
  *     fn(count, mean, M2, newValue):
  *         (count, mean, M2) = existingAggregate
  *         count += 1
  *         delta = newValue - mean
  *         mean += delta / count
  *         delta2 = newValue - mean
  *         M2 += delta * delta2
  *         return (count, mean, M2)
 */
Status
MeanAggr::Accumulate(shared_ptr<Table> new_vals, int64_t col_ndx) {
    shared_ptr<ChunkedArray> delta_mean;
    shared_ptr<ChunkedArray> delta_var;

    if (this->count == 0) {
        // std::cout << "Initializing new values..." << std::endl;
        this->Initialize(new_vals->column(col_ndx));
        this->count  = 1;
        col_ndx     += 1;
    }

    for (; col_ndx < new_vals->num_columns(); col_ndx++) {
        // use a result just to make call chains easier
        auto col_vals = new_vals->column(col_ndx);

        this->count += 1;
        delta_mean  = VecSub(col_vals, this->means);
        this->means = VecAdd(
             this->means
            ,VecDiv(delta_mean, Datum(this->count))
        );

        delta_var       = VecSub(col_vals, this->means);
        this->variances = VecAdd(
             this->variances
            ,VecMul(delta_var, delta_mean)
        );
    }

    return Status::OK();
}


/**
 * Assumes rows correspond to each other
 * Per Wikipedia (parallel variance):
 *     n      = n_a + n_b
 *     delta  = avg_b - avg_a
 *     M2     = M2_a + M2_b + delta ** 2 * n_a * n_b / n
 *     var_ab = M2 / (n - 1)
 *
 *     return var_ab
 */
Status
MeanAggr::Combine(const MeanAggr *other_aggr) {
    // sum the sample sizes
    int64_t total_count = this->count + other_aggr->count;

    std::cout << "Dimensions of [A]:" << std::endl
              << "\tmeans: "     << this->means->length()     << std::endl
              << "\tvariances: " << this->variances->length() << std::endl
    ;

    std::cout << "Dimensions of [B]:" << std::endl
              << "\tmeans: "     << other_aggr->means->length()     << std::endl
              << "\tvariances: " << other_aggr->variances->length() << std::endl
    ;

    // >> update mean
    auto this_sum  = VecMul(this->means      , Datum(this->count      ));
    auto other_sum = VecMul(other_aggr->means, Datum(other_aggr->count));
    auto new_means = VecDiv(VecAdd(this_sum, other_sum), Datum(total_count));
    this->means    = new_means;

    // >> update M2
    // Recurrent term
    auto delta_mean    = VecSub(other_aggr->means, this->means);
    auto squared_delta = VecMul(delta_mean       , delta_mean );
    auto co_samplesize = (this->count * other_aggr->count) / total_count;
    auto co_variance   = VecMul(squared_delta, Datum(co_samplesize));

    // M2,a + M2,b + recurrent term
    auto combined_vars = VecAdd(this->variances, other_aggr->variances);
    auto new_variance  = VecAdd(combined_vars  , co_variance          );
    this->variances    = new_variance;

    return Status::OK();
}

Status
MeanAggr::PrintState() {
    auto current_state = this->TakeResult();

    std::cout << "samples aggregated: " << this->count << std::endl;
    std::cout << "current aggregates:" << std::endl
              << "+-----------------+" << std::endl
              << current_state->ToString()
              << "+-----------------+" << std::endl
              << std::endl
    ;

    return Status::OK();
}

Status
MeanAggr::PrintM1M2() {
    auto final_vars = VecDiv(this->variances, Datum(this->count));

    std::cout << "samples aggregated: " << this->count << std::endl;

    std::cout << "___________________" << std::endl
              << "sample means:"       << std::endl
              << this->means->ToString()
              << std::endl
              << std::endl
    ;

    std::cout << "___________________" << std::endl
              << "sample variances:"   << std::endl
              << final_vars->ToString()
              << std::endl
              << std::endl
    ;

    return Status::OK();
}
