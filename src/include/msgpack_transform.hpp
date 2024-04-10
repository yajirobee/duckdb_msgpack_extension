#pragma once

#include <msgpack.hpp>

#include "duckdb/common/types/vector.hpp"

namespace duckdb {
bool MsgpackTransform(msgpack::object *values[], Vector &result, const idx_t count);
}
