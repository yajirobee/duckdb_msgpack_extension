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
    values[row_idx]->convert<T>(data[row_idx]);
  }
  return true;
}

static bool TransformFromString(msgpack::object *values[], Vector &result,
                                const idx_t count) {
  Vector string_vector(LogicalTypeId::VARCHAR, count);

  auto data = FlatVector::GetData<string_t>(string_vector);
  for (idx_t row_idx = 0; row_idx < count; row_idx++) {
    values[row_idx]->convert<string_t>(data[row_idx]);
  }

  return VectorOperations::DefaultTryCast(string_vector, result, count, nullptr, true);
}

bool Transform(msgpack::object *values[], Vector &result, const idx_t count) {
  auto result_type = result.GetType();

  // TODO: use cast_operators of DuckDB
  switch (result_type.id()) {
  case LogicalTypeId::SQLNULL:
    return true;
  case LogicalTypeId::BOOLEAN:
    return TransformVals<bool>(values, result, count);
  case LogicalTypeId::TINYINT:
    return TransformVals<int8_t>(values, result, count);
  case LogicalTypeId::SMALLINT:
    return TransformVals<int16_t>(values, result, count);
  case LogicalTypeId::INTEGER:
    return TransformVals<int32_t>(values, result, count);
  case LogicalTypeId::BIGINT:
    return TransformVals<int64_t>(values, result, count);
  case LogicalTypeId::UTINYINT:
    return TransformVals<uint8_t>(values, result, count);
  case LogicalTypeId::USMALLINT:
    return TransformVals<uint16_t>(values, result, count);
  case LogicalTypeId::UINTEGER:
    return TransformVals<uint32_t>(values, result, count);
  case LogicalTypeId::UBIGINT:
    return TransformVals<uint64_t>(values, result, count);
  case LogicalTypeId::FLOAT:
    return TransformVals<float>(values, result, count);
  case LogicalTypeId::DOUBLE:
    return TransformVals<double>(values, result, count);
  case LogicalTypeId::AGGREGATE_STATE:
  case LogicalTypeId::ENUM:
  case LogicalTypeId::DATE:
  case LogicalTypeId::INTERVAL:
  case LogicalTypeId::TIME:
  case LogicalTypeId::TIME_TZ:
  case LogicalTypeId::TIMESTAMP:
  case LogicalTypeId::TIMESTAMP_TZ:
  case LogicalTypeId::TIMESTAMP_NS:
  case LogicalTypeId::TIMESTAMP_MS:
  case LogicalTypeId::TIMESTAMP_SEC:
  case LogicalTypeId::UUID:
    return TransformFromString(values, result, count);
  case LogicalTypeId::CHAR:
    return TransformVals<string_t>(values, result, count);
  case LogicalTypeId::VARCHAR:
    return TransformVals<string_t>(values, result, count);
  default:
    throw NotImplementedException(
        "Cannot read a value of type %s from a msgpack file",
        result_type.ToString());
  }
}
} // namespace duckdb
