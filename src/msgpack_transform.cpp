#include "msgpack_transform.hpp"

#include <iostream>

#include "duckdb/common/types.hpp"

namespace duckdb {
bool Transform(msgpack::object *values[], Vector &result, const idx_t count) {
  auto result_type = result.GetType();
  std::cout << "result_type: " << result_type.ToString() << std::endl;

  switch (result_type.id()) {
  case LogicalTypeId::INTEGER: {
    auto data = FlatVector::GetData<int32_t>(result);
    for (idx_t row_idx = 0; row_idx < count; row_idx++) {
      data[row_idx] = values[row_idx]->as<int32_t>();
    }
    return true;
  }
  default:
    throw NotImplementedException(
        "Cannot read a value of type %s from a json file",
        result_type.ToString());
  }
}
} // namespace duckdb
