//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/scoped_hash_join_timer.hpp
//
// A small RAII helper for accumulating nanosecond timings into an optional
// uint64_t counter.
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <chrono>

namespace duckdb {

class ScopedHashJoinTimer {
public:
	explicit ScopedHashJoinTimer(uint64_t *target_p)
	    : target(target_p), start(std::chrono::steady_clock::now()) {
	}

	~ScopedHashJoinTimer() {
		if (!target) {
			return;
		}
		auto end = std::chrono::steady_clock::now();
		auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
		*target += static_cast<uint64_t>(elapsed_ns);
	}

private:
	uint64_t *target;
	std::chrono::steady_clock::time_point start;
};

} // namespace duckdb

