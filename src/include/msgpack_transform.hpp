#pragma once

#include <msgpack.hpp>

#include "duckdb/function/scalar/strftime_format.hpp"

namespace duckdb {
bool Transform(msgpack::object *values[], Vector &result, const idx_t count);
}
