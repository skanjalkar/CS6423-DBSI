
#include "optimizer/table_stats.h"
#include <cstddef>
#include <cstdint>
#include "operators/seq_scan.h"
#include <set>

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
IntHistogram::IntHistogram(UNUSED_ATTRIBUTE int64_t buckets, 
    UNUSED_ATTRIBUTE int64_t min_val,
    UNUSED_ATTRIBUTE int64_t max_val) {
    this->buckets_ = buckets;
    this->min_val_ = min_val;
    this->max_val_ = max_val;
    this->bucket_width_ = std::max(1.0, (max_val - min_val + 1.0) / buckets);
    this->heights_.resize(buckets, 0);
    this->total_tuples_ = 0;
}

/**
 * Add a value to the set of values that you are keeping a histogram of.
 * @param val Value to add to the histogram
 */
void IntHistogram::add_value(UNUSED_ATTRIBUTE int64_t val) {
    if (val >= min_val_ && val <= max_val_) {
        int bucket_index = static_cast<int>((val - min_val_) / bucket_width_);
        if (bucket_index >= buckets_) {
            bucket_index = buckets_ - 1;
        }
        heights_[bucket_index]++;
        total_tuples_++;
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
        
    if (total_tuples_ == 0) {
        return 1.0;
    }
    
    if (v < min_val_) {
        switch (op) {
            case PredicateType::LT:
            case PredicateType::LE:
                return 0.0;  // No values are less than min_val_
            case PredicateType::GT:
            case PredicateType::GE:
                return 1.0;  // All values are greater than anything below min_val_
            case PredicateType::EQ:
                return 0.0;  // No values equal to anything below min_val_
            case PredicateType::NE:
                return 1.0;  // All values not equal to anything below min_val_
            default:
                return 1.0;
        }
    }

    if (v > max_val_) {
        switch (op) {
            case PredicateType::LT:
            case PredicateType::LE:
                return 1.0;  // All values are less than anything above max_val_
            case PredicateType::GT:
            case PredicateType::GE:
                return 0.0;  // No values are greater than max_val_
            case PredicateType::EQ:
                return 0.0;  // No values equal to anything above max_val_
            case PredicateType::NE:
                return 1.0;  // All values not equal to anything above max_val_
            default:
                return 1.0;
        }
    }
    
    int bucket_index = static_cast<int>((v - min_val_) / bucket_width_);
    if (bucket_index >= buckets_) {
        bucket_index = buckets_ - 1;
    }
    
    double bucket_selectivity = static_cast<double>(heights_[bucket_index]) / total_tuples_;
    double bucket_left = min_val_ + bucket_index * bucket_width_;
    double fraction_in_bucket = (v - bucket_left) / bucket_width_; // Position within bucket

    switch (op) {
        case PredicateType::EQ: {
            if (heights_[bucket_index] == 0) return 0.0;
            return bucket_selectivity / bucket_width_;
        }
        case PredicateType::NE: {
            if (heights_[bucket_index] == 0) return 1.0;
            return 1.0 - (bucket_selectivity / bucket_width_);
        }
        case PredicateType::GT:
        case PredicateType::GE: {
            double selectivity = 0.0;
            
            if (heights_[bucket_index] > 0) {
                if (op == PredicateType::GT) {
                    selectivity += bucket_selectivity * (1.0 - fraction_in_bucket);
                } else { // GE
                    selectivity += bucket_selectivity * (1.0 - fraction_in_bucket + 1.0/bucket_width_);
                }
            }
            
            // Add contribution from all buckets to the right
            for (int i = bucket_index + 1; i < buckets_; i++) {
                selectivity += static_cast<double>(heights_[i]) / total_tuples_;
            }
            
            return selectivity;
        }
        case PredicateType::LT:
        case PredicateType::LE: {
            // Add selectivity from buckets to left of v
            double selectivity = 0.0;
            
            // Add partial contribution from current bucket
            if (heights_[bucket_index] > 0) {
                if (op == PredicateType::LT) {
                    selectivity += bucket_selectivity * fraction_in_bucket;
                } else { // LE
                    selectivity += bucket_selectivity * (fraction_in_bucket + 1.0/bucket_width_);
                }
            }
            
            // Add contribution from all buckets to the left
            for (int i = 0; i < bucket_index; i++) {
                selectivity += static_cast<double>(heights_[i]) / total_tuples_;
            }
            
            return selectivity;
        }
        default:
            return 1.0; // Unknown operator
    }
    
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
TableStats::TableStats(UNUSED_ATTRIBUTE int64_t table_id, 
    UNUSED_ATTRIBUTE int64_t io_cost_per_page,
    UNUSED_ATTRIBUTE uint64_t num_pages,
    UNUSED_ATTRIBUTE uint64_t num_fields) {
    /*
        // some code goes here
        Hint: use seqscan operator (operators/seq_scan.h) to scan over the table
        and build stats. You should try to do this reasonably efficiently, but you don't
        necessarily have to (for example) do everything in a single scan of the table.
    */
    
    this->table_id_ = table_id;
    this->io_cost_per_page_ = io_cost_per_page;
    this->num_pages_ = num_pages;
    this->num_fields_ = num_fields;
    this->tuple_count_= 0;
    distinct_values_.resize(num_fields, 0);
    
    std::vector<int64_t> mins(num_fields, INT64_MAX);
    std::vector<int64_t> maxs(num_fields, INT64_MIN);
    std::vector<std::set<int64_t>> unique_values(num_fields);
    operators::SeqScan scanner(table_id, num_pages, num_fields);
    scanner.open();
    while (scanner.has_next()) {
        auto tuple = scanner.get_tuple();
        ++tuple_count_;
        for (size_t i = 0; i < num_fields; ++i) {
            int64_t value = tuple[i];
            mins[i] = std::min(mins[i], value);
            maxs[i] = std::max(maxs[i], value);
            unique_values[i].insert(value);
        }
    }
    
    histograms_.resize(num_fields);
    for (size_t i = 0; i < num_fields; ++i) {
        distinct_values_[i] = unique_values[i].size();
        if (mins[i] <= maxs[i]) {
            histograms_[i] = IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
        } else {
            histograms_[i] = IntHistogram(1, 0, 1);
        }
    }
    
    scanner.reset();
    while (scanner.has_next()) {
        auto tuple = scanner.get_tuple();
        for (size_t i = 0; i < num_fields; ++i) {
            histograms_[i].add_value(tuple[i]);
        }
    }
    
    scanner.close();
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

double TableStats::estimate_scan_cost() {
    return io_cost_per_page_ * num_pages_;
}

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
    return static_cast<uint64_t>(tuple_count_ * selectivity_factor);
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
    if (field < 0 || field >= static_cast<int64_t>(histograms_.size())) {
        return 1.0;
    }
    return histograms_[field].estimate_selectivity(op, constant);
}

}  // namespace table_stats
}  // namespace buzzdb