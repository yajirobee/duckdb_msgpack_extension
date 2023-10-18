#include "msgpack_transform.hpp"

#include <iostream>

#include "duckdb/common/types.hpp"

namespace duckdb {
template <class T>
static inline bool TransformVals(msgpack::object *values[], Vector &result,
                                 const idx_t count) {
  auto data = FlatVector::GetData<T>(result);
  // TODO: set validity
  // TODO: handle type error
  for (idx_t row_idx = 0; row_idx < count; row_idx++) {
    data[row_idx] = values[row_idx]->as<T>();
  }
  return true;
}

bool Transform(msgpack::object *values[], Vector &result, const idx_t count) {
  auto result_type = result.GetType();
  std::cout << "result_type: " << result_type.ToString() << std::endl;

  // TODO: use cast_operators of DuckDB
  switch (result_type.id()) {
  case LogicalTypeId::SQLNULL:
    return true;
  case LogicalTypeId::BOOLEAN:
    return TransformVals<bool>(values, result, count);
  case LogicalTypeId::INTEGER:
    return TransformVals<int32_t>(values, result, count);
  default:
    throw NotImplementedException(
        "Cannot read a value of type %s from a json file",
        result_type.ToString());
  }
}
} // namespace duckdb
