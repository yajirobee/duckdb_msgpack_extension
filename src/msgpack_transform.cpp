#include "msgpack_transform.hpp"

#include <iostream>

#include "duckdb/common/types.hpp"

namespace duckdb {
  bool Transform(msgpack::object &value, Vector &result, const idx_t row_idx) {
    auto result_type = result.GetType();
    std::cout << "result_type: " << result_type.ToString() << std::endl;

    switch (result_type.id()) {
    case LogicalTypeId::INTEGER:
      auto data = FlatVector::GetData<int32_t>(result);
      data[row_idx] = value.as<int32_t>();

    default:
      throw NotImplementedException(
          "Cannot read a value of type %s from a json file",
          result_type.ToString());
    }
    return true;
  }
}

