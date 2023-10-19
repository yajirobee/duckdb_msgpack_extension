#include "msgpack_transform.hpp"

#include <iostream>

#include "duckdb/common/types.hpp"

namespace msgpack {
MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
  namespace adaptor {
  template <> struct convert<duckdb::string_t> {
    msgpack::object const &operator()(msgpack::object const &o,
                                      duckdb::string_t &v) const {
      switch (o.type) {
      case msgpack::type::BIN:
        v = duckdb::string_t(o.via.bin.ptr, o.via.bin.size);
        break;
      case msgpack::type::STR:
        v = duckdb::string_t(o.via.str.ptr, o.via.str.size);
        break;
      default:
        throw msgpack::type_error();
        break;
      }
      return o;
    }
  };
  } // namespace adaptor
}
} // namespace msgpack

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

static inline bool TransformStrings(msgpack::object *values[], Vector &result,
                                    const idx_t count) {
  auto data = FlatVector::GetData<string_t>(result);
  for (idx_t row_idx = 0; row_idx < count; row_idx++) {
    values[row_idx]->convert<string_t>(data[row_idx]);
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
  case LogicalTypeId::VARCHAR:
    return TransformStrings(values, result, count);
  default:
    throw NotImplementedException(
        "Cannot read a value of type %s from a json file",
        result_type.ToString());
  }
}
} // namespace duckdb
