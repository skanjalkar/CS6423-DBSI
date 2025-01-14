#include "external_sort/external_sort.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
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

/*
FROM WIKI...

For example, for sorting 900 megabytes of data using only 100 megabytes of RAM:

    1. Read 100 MB of the data in main memory and sort by some conventional method, like quicksort.

    2. Write the sorted data to disk.

    3. Repeat steps 1 and 2 until all of the data is in sorted 100 MB chunks
    (there are 900MB / 100MB = 9 chunks), which now need to be merged into one single output file.

    4. Read the first 10 MB (= 100MB / (9 chunks + 1)) of each sorted chunk into input buffers in main memory
    and allocate the remaining 10 MB for an output buffer. (In practice, it might provide better
    performance to make the output buffer larger and the input buffers slightly smaller.)

    5. Perform a 9-way merge and store the result in the output buffer. Whenever the output buffer fills,
    write it to the final sorted file and empty it. Whenever any of the 9 input buffers empties,
    fill it with the next 10 MB of its associated 100 MB sorted chunk until no more data from the chunk is available.
*/

namespace buzzdb {

struct ChunkData {
    uint64_t value;
    size_t chunk_index;
    size_t index;

    bool operator>(const ChunkData &other) const {
        return value > other.value;
    }
};

void external_sort(File &input, size_t num_values, File &output, size_t mem_size) {
    /* To be implemented
    ** Remove these before you start your implementation
    */

    std::vector<std::unique_ptr<File>> chunk_files;
    size_t chunk_size = mem_size / sizeof(uint64_t);
    std::vector<uint64_t> buffer(chunk_size);
    std::cout << "Chunk size: " << chunk_size << std::endl;


    for (size_t offset = 0; offset < num_values; offset += chunk_size) {
        auto chunk_file = File::make_temporary_file();
        size_t current_chunk_size = std::min(chunk_size, num_values - offset);

        input.read_block(
            offset * sizeof(uint64_t),
            current_chunk_size * sizeof(uint64_t),
            reinterpret_cast<char*>(buffer.data())
        );
        std::sort(buffer.begin(), buffer.end(), [&](const uint64_t a, const uint64_t b) {
            return a < b;
        });
        chunk_file->resize(current_chunk_size*sizeof(uint64_t));
        chunk_file->write_block(
            reinterpret_cast<char*>(buffer.data()),
            0,
            current_chunk_size * sizeof(uint64_t)
        );
        chunk_files.push_back(std::move(chunk_file));
        // print the chunk
        // std::cout << "Printing data " << std::endl;
        // for (size_t i = 0; i < current_chunk_size; i++) {
        //    std::cout << buffer[i] << " ";
        // }
        // std::cout << std::endl;
    }

    // k way merge

    std::priority_queue<ChunkData,
        std::vector<ChunkData>,
        std::greater<ChunkData>> pq;


    for (size_t i = 0; i < chunk_files.size(); ++i) {
        uint64_t value;
        chunk_files[i]->read_block(0, sizeof(uint64_t), reinterpret_cast<char*>(&value));
        pq.push({value, i, 0});
    }

    size_t output_pos = 0;
    while (!pq.empty()) {
        ChunkData current = pq.top();
        pq.pop();
        output.write_block(
            reinterpret_cast<char*>(&current.value),
            output_pos,
            sizeof(uint64_t)
        );
        output_pos += sizeof(uint64_t);

        if (current.index + 1 < chunk_size) {
            uint64_t value;
            chunk_files[current.chunk_index]->read_block(
                (current.index + 1) * sizeof(uint64_t),
                sizeof(uint64_t),
                reinterpret_cast<char*>(&value)
            );
            pq.push({value, current.chunk_index, current.index + 1});
        }
    }

    // no need to remove temp files, unique ptr
    // should remove automatically
}

}  // namespace buzzdb
