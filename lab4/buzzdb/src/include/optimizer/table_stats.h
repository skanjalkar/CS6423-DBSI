#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>

#include "operators/seq_scan.h"

using buzzdb::operators::PredicateType;

namespace buzzdb {
namespace table_stats {

class IntHistogram {
private:
    int64_t buckets_;
    int64_t min_val_;
    int64_t max_val_;
    double bucket_width_;
    std::vector<int> heights_;
    int64_t total_tuples_;

public:
    IntHistogram() = default;

    IntHistogram(int64_t buckets, int64_t min_val, int64_t max_val);

    double estimate_selectivity(PredicateType op, int64_t v);
    void add_value(int64_t val);
};

class TableStats {
   public:
    TableStats() = default;
    TableStats(int64_t table_id, int64_t io_cost_per_page, uint64_t num_pages, uint64_t num_fields);
    double estimate_selectivity(int64_t field, PredicateType op, int64_t constant);
    double estimate_scan_cost();
    uint64_t estimate_table_cardinality(double selectivity_factor);
    
    int get_distinct_values(int field) const {
        if (field < 0 || field >= static_cast<int>(distinct_values_.size())) {
            return 1; // Default if field is invalid
        }
        return distinct_values_[field];
    }

   private:
    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    int NUM_HIST_BINS = 100;
    int64_t table_id_;
    int64_t io_cost_per_page_;
    uint64_t num_pages_;
    uint64_t num_fields_;
    uint64_t tuple_count_;
    std::vector<IntHistogram> histograms_;
    std::vector<int> distinct_values_;
};

}  // namespace table_stats
}  // namespace buzzdb