#include "external_sort/external_sort.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

#include "storage/file.h"

#define UNUSED(p) ((void)(p))

namespace buzzdb {

/**
 * A small struct to help manage reading from each chunk (run).
 * see https://stackoverflow.com/questions/20802396/how-external-merge-sort-algorithm-works
 * We maintain:
 *  - The file pointer to the chunk.
 *  - The total number of 64-bit values in that chunk.
 *  - Current offset (# of values) read so far from the chunk.
 *  - An in-memory buffer for partial reads of the chunk.
 *  - The current index (position) within that buffer.
 */
struct ChunkReader {
    std::unique_ptr<File> file;
    size_t total_values;           // total number of uint64_t in the chunk
    size_t values_read;            // how many values read so far from the chunk
    std::vector<uint64_t> buffer;  // buffer for k-way merge
    size_t buffer_idx;             // current position in buffer

    // Constructor
    ChunkReader(std::unique_ptr<File> f, size_t num_vals)
        : file(std::move(f)),
          total_values(num_vals),
          values_read(0),
          buffer_idx(0) {}
};

/**
 * loads the next batch of data (up to 'batch_size' 64-bit values)
 * from the chunk file into the chunk buffer and returns the number of new values loaded.
 */
size_t load_next_batch(ChunkReader &cr, size_t batch_size) {
    // Determine how many values are left in this chunk
    size_t remaining = cr.total_values - cr.values_read;
    if (remaining == 0) {
        return 0; // no more data to load
    }

    // We'll read either 'batch_size' or 'remaining', whichever is smaller
    size_t to_read = std::min(batch_size, remaining);

    cr.buffer.resize(to_read);
    cr.buffer_idx = 0; // reset index in buffer

    cr.file->read_block(
        cr.values_read * sizeof(uint64_t),    // offset in bytes
        to_read * sizeof(uint64_t),           // size in bytes
        reinterpret_cast<char*>(cr.buffer.data())
    );

    cr.values_read += to_read;
    return to_read;
}

void k_way_merge(std::vector<ChunkReader> &chunk_readers,
                 File &output,
                 size_t mem_size) {
    // Number of chunks (runs)
    size_t write_offset = 0;
    size_t num_chunks = chunk_readers.size();
    if (num_chunks == 0) {
        return; // nothing to merge
    }

    // We want to allocate some memory for:
    //   1) The output buffer
    //   2) The chunk buffers (one for each chunk)
    //
    // Simple approach from the example: divide memory equally among
    // all chunk readers + 1 output buffer.
    // SEE: https://en.wikipedia.org/wiki/External_sorting
    // https://web.archive.org/web/20150208064321/http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX1.htm
    // https://web.archive.org/web/20150202022830/http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
    // NOTE: each entry is sizeof(uint64_t) bytes, so we compute how many
    // 64-bit values can fit in 'mem_size'.
    size_t total_capacity_in_values = mem_size / sizeof(uint64_t);
    if (total_capacity_in_values < num_chunks + 1) {
        // Not enough memory to have 1 value per chunk + 1 in output buffer
        // This is a corner case: you'd normally want at least some memory.
        // We'll just bail or throw an error for simplicity.
        throw std::runtime_error(
            "Not enough memory to perform a multi-way merge for all runs."
        );
    }

    // We'll let each chunk have a buffer of size chunk_buf_size, and the output
    // buffer be out_buf_size.
    // One naive approach is to let them be all the same size. Another approach
    // is from the wiki: each chunk buffer is mem_size/(num_chunks+1).
    size_t chunk_buf_size = total_capacity_in_values / (num_chunks + 1);
    size_t out_buf_size   = chunk_buf_size; // we keep it the same for simplicity

    // Prepare an output buffer
    std::vector<uint64_t> out_buffer;
    out_buffer.reserve(out_buf_size);
    out_buffer.clear();

    // 1) Load initial batch from each chunk
    for (auto &cr : chunk_readers) {
        load_next_batch(cr, chunk_buf_size);
    }

    // 2) Build a min-heap (priority queue) that will store
    //    (current_value, chunk_id)
    //    so that we can always pop the smallest value across all chunks.
    using PQItem = std::pair<uint64_t, size_t>;  // (value, which_chunk)
    auto cmp = [](const PQItem &a, const PQItem &b) {
        return a.first > b.first; // min-heap based on 'value'
    };
    std::priority_queue<PQItem, std::vector<PQItem>, decltype(cmp)> min_heap(cmp);

    // Push the front element of each chunk (if not empty) into the heap
    for (size_t i = 0; i < num_chunks; i++) {
        if (!chunk_readers[i].buffer.empty()) {
            min_heap.push({chunk_readers[i].buffer[0], i});
        }
    }

    // 3) Repeatedly pop from the heap, output the smallest, then read next from that chunk
    while (!min_heap.empty()) {
        // Pop the smallest element
        auto [val, cid] = min_heap.top();
        min_heap.pop();

        // Push to the out_buffer
        out_buffer.push_back(val);
        if (out_buffer.size() == out_buf_size) {
            size_t bytes_to_write = out_buf_size * sizeof(uint64_t);
            // Resize so we can safely write
            output.resize(write_offset + bytes_to_write);
            // Actually write
            output.write_block(
                reinterpret_cast<char*>(out_buffer.data()),
                write_offset,
                bytes_to_write
            );
            // Increment offset
            write_offset += bytes_to_write;

            out_buffer.clear();
        }

        // Move that chunk's buffer index forward
        ChunkReader &cr = chunk_readers[cid];
        cr.buffer_idx++;
        // If the chunk buffer is exhausted, we load more from that chunk file
        if (cr.buffer_idx >= cr.buffer.size()) {
            // load next batch
            size_t loaded = load_next_batch(cr, chunk_buf_size);
            if (loaded > 0) {
                // we have new data, so push the front of this new batch
                min_heap.push({cr.buffer[0], cid});
            }
        } else {
            // The chunk still has elements in the buffer, push next element
            min_heap.push({cr.buffer[cr.buffer_idx], cid});
        }
    }

    // 4) Flush any remaining data in out_buffer
    if (!out_buffer.empty()) {
        size_t bytes_to_write = out_buffer.size() * sizeof(uint64_t);
        output.resize(write_offset + bytes_to_write);
        output.write_block(
            reinterpret_cast<char*>(out_buffer.data()),
            write_offset,
            bytes_to_write
        );
        write_offset += bytes_to_write;
        out_buffer.clear();
    }
}

/**
 * External sort:
 *  - Phase 1: Break into sorted runs (already shown).
 *  - Phase 2: Merge those runs (k-way merge).
 */
void external_sort(File &input, size_t num_values, File &output, size_t mem_size) {
    // ============ Phase 1: Sort chunks in-memory and write them out ============
    std::vector<std::unique_ptr<File>> chunk_files;
    std::vector<size_t> chunk_sizes; // how many values in each run

    // how many 64-bit integers fit in 'mem_size'
    size_t chunk_size = mem_size / sizeof(uint64_t);
    std::vector<uint64_t> buffer(chunk_size);

    std::cout << "[Phase 1] chunk_size (in 64-bit values) = " << chunk_size << std::endl;

    for (size_t offset = 0; offset < num_values; offset += chunk_size) {
        // create a new temporary file for each run
        chunk_files.push_back(File::make_temporary_file());
        size_t current_chunk_size = std::min(chunk_size, num_values - offset);

        input.read_block(
            offset * sizeof(uint64_t),
            current_chunk_size * sizeof(uint64_t),
            reinterpret_cast<char*>(buffer.data())
        );

        // sort in-memory
        std::sort(buffer.begin(), buffer.begin() + current_chunk_size);
        chunk_files.back()->resize(current_chunk_size * sizeof(uint64_t));
        // write out the sorted chunk
        chunk_files.back()->write_block(
            reinterpret_cast<char*>(buffer.data()),
            0,
            current_chunk_size * sizeof(uint64_t)
        );

        // keep track of how many values are in this chunk
        chunk_sizes.push_back(current_chunk_size);

        // (Optional) debug print
        std::cout << "[Phase 1] Created sorted run with " << current_chunk_size << " values\n";
    }

    // ============ Phase 2: K-way merge all chunk files into 'output' ============
    std::vector<ChunkReader> chunk_readers;
    chunk_readers.reserve(chunk_files.size());
    for (size_t i = 0; i < chunk_files.size(); i++) {
        chunk_readers.emplace_back(std::move(chunk_files[i]), chunk_sizes[i]);
    }

    // Now do the k-way merge
    // stupid bug because of resize
    // should not assume that specified size will meet requirement
    // https://buzzdb-docs.readthedocs.io/part2/lab1.html point 5
    // 5. Do not forget to resize the temporary file as needed.

    k_way_merge(chunk_readers, output, mem_size);

    std::cout << "[Phase 2] Merge completed. Final sorted data is in 'output'.\n";
}

}  // namespace buzzdb
