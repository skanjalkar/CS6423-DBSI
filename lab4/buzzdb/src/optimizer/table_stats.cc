
#include "optimizer/table_stats.h"

#include <climits>
#include <unordered_set>
#include <algorithm>
#include <cmath>

namespace buzzdb {
namespace table_stats {

/**
 * Create a new IntHistogram.
 *
 * This IntHistogram should maintain a histogram of integer values that it receives.
 * It should split the histogram into "buckets" buckets.
 *
 * The values that are being histogrammed will be provided one-at-a-time through the "add_value()"
 * function.
 *
 * Your implementation should use space and have execution time that are both
 * constant with respect to the number of values being histogrammed.  For example, you shouldn't
 * simply store every value that you see in a sorted list.
 *
 * @param buckets The number of buckets to split the input value into.
 * @param min_val The minimum integer value that will ever be passed to this class for histogramming
 * @param max_val The maximum integer value that will ever be passed to this class for histogramming
 */
IntHistogram::IntHistogram(UNUSED_ATTRIBUTE int64_t buckets, UNUSED_ATTRIBUTE int64_t min_val,
                           UNUSED_ATTRIBUTE int64_t max_val) {
    histogram.reserve(buckets);
    this->buckets = buckets;
    this->min_val = min_val;
    this->max_val = max_val;
    this->keys_per_bin = (max_val - min_val) / buckets + 1;
}

/**
 * Add a value to the set of values that you are keeping a histogram of.
 * @param val Value to add to the histogram
 */
void IntHistogram::add_value(UNUSED_ATTRIBUTE int64_t val) {
    int64_t bin = (val - min_val) / keys_per_bin;
    if (histogram.find(bin) == histogram.end()) {
        histogram[bin] = 1;
    } else {
        histogram[bin]++;
    }
}

/**
 * Estimate the selectivity of a particular predicate and operand on this table.
 *
 * For example, if "op" is "GREATER_THAN" and "v" is 5,
 * return your estimate of the fraction of elements that are greater than 5.
 *
 * @param op Operator
 * @param v Value
 * @return Predicted selectivity of this particular operator and value
 */
double IntHistogram::estimate_selectivity(UNUSED_ATTRIBUTE PredicateType op,
                                          UNUSED_ATTRIBUTE int64_t v) {
    int64_t total_values = 0;
    for (const auto &pair : histogram) {
        total_values += pair.second;
    }
    switch (op) {
        case PredicateType::EQ: {
            int64_t bin = (v - min_val) / keys_per_bin;
            return static_cast<double>(histogram[bin]) / total_values;
        }
        case PredicateType::NE: {
            return 1.0 - estimate_selectivity(PredicateType::EQ, v);
        }
        case PredicateType::LT: {
            int64_t bin_lt = (v - min_val) / keys_per_bin;
            int64_t min_key_in_bin = min_val + bin_lt * keys_per_bin;
            int64_t included_keys_in_bin = v - min_key_in_bin;
            int64_t included_values = histogram[bin_lt] * included_keys_in_bin / keys_per_bin;
            for (int64_t i = 0; i < bin_lt; i++) {
                included_values += histogram[i];
            }
            return static_cast<double>(included_values) / total_values;
        }
        case PredicateType::LE: {
            return estimate_selectivity(PredicateType::LT, v) +
                   estimate_selectivity(PredicateType::EQ, v);
        }
        case PredicateType::GT: {
            return 1.0 - estimate_selectivity(PredicateType::LE, v);
        }
        case PredicateType::GE: {
            return 1.0 - estimate_selectivity(PredicateType::LT, v);
        }
    }
    return -1;
}

/**
 * Create a new TableStats object, that keeps track of statistics on each
 * column of a table
 *
 * @param table_id
 *            The table over which to compute statistics
 * @param io_cost_per_page
 *            The cost per page of IO. This doesn't differentiate between
 *            sequential-scan IO and disk seeks.
 * @param num_pages
 *            The number of disk pages spanned by the table
 * @param num_fields
 *            The number of columns in the table
 */
TableStats::TableStats(UNUSED_ATTRIBUTE int64_t table_id, UNUSED_ATTRIBUTE int64_t io_cost_per_page,
                       UNUSED_ATTRIBUTE uint64_t num_pages, UNUSED_ATTRIBUTE uint64_t num_fields) {
    this->num_pages = num_pages;
    this->num_fields = num_fields;
    this->io_cost_per_page = io_cost_per_page;
    operators::SeqScan seq_scan(table_id, num_pages, num_fields);
    seq_scan.open();
    std::vector min_vals(num_fields, INT_MAX);
    std::vector max_vals(num_fields, INT_MIN);

    while (seq_scan.has_next()) {
        for (uint64_t i = 0; i < num_fields; i++) {
            std::vector<int> tuple = seq_scan.get_tuple();
            min_vals[i] = std::min(min_vals[i], tuple[i]);
            max_vals[i] = std::max(max_vals[i], tuple[i]);
        }
    }

    seq_scan.reset();

    this->histograms.reserve(num_fields);
    for (uint64_t i = 0; i < num_fields; i++) {
        histograms.emplace_back(NUM_HIST_BINS, min_vals[i], max_vals[i]);
    }
    this->num_tuples = 0;
    while (seq_scan.has_next()) {
        std::vector<int> tuple = seq_scan.get_tuple();
        for (uint64_t i = 0; i < num_fields; i++) {
            histograms[i].add_value(tuple[i]);
        }
        num_tuples++;
    }
    seq_scan.close();
    /*
        // some code goes here
        Hint: use seqscan operator (operators/seq_scan.h) to scan over the table
        and build stats. You should try to do this reasonably efficiently, but you don't
        necessarily have to (for example) do everything in a single scan of the table.
    */
}

/**
 * Estimates the cost of sequentially scanning the file, given that the cost
 * to read a page is io_cost_per_page. You can assume that there are no seeks
 * and that no pages are in the buffer pool.
 *
 * Also, assume that your hard drive can only read entire pages at once, so
 * if the last page of the table only has one tuple on it, it's just as
 * expensive to read as a full page. (Most real hard drives can't
 * efficiently address regions smaller than a page at a time.)
 *
 * @return The estimated cost of scanning the table.
 */

double TableStats::estimate_scan_cost() { return num_pages * io_cost_per_page; }

/**
 * This method returns the number of tuples in the relation, given that a
 * predicate with selectivity selectivity_factor is applied.
 *
 * @param selectivityFactor
 *            The selectivity of any predicates over the table
 * @return The estimated cardinality of the scan with the specified
 *         selectivity_factor
 */
uint64_t TableStats::estimate_table_cardinality(UNUSED_ATTRIBUTE double selectivity_factor) {
    return num_tuples * selectivity_factor;
}

/**
 * Estimate the selectivity of predicate <tt>field op constant</tt> on the
 * table.
 *
 * @param field
 *            The field over which the predicate ranges
 * @param op
 *            The logical operation in the predicate
 * @param constant
 *            The value against which the field is compared
 * @return The estimated selectivity (fraction of tuples that satisfy) the
 *         predicate
 */
double TableStats::estimate_selectivity(UNUSED_ATTRIBUTE int64_t field,
                                        UNUSED_ATTRIBUTE PredicateType op,
                                        UNUSED_ATTRIBUTE int64_t constant) {
    return histograms[field].estimate_selectivity(op, constant);
}

}  // namespace table_stats
}  // namespace buzzdb