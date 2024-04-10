#include "msgpack_transform.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

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
static inline bool GetValueNumerical(msgpack::object const &obj, T &result) {
  D_ASSERT(obj.type != msgpack::type::NIL);
  switch (obj.type) {
  case msgpack::type::BOOLEAN:
    return TryCast::Operation<bool, T>(obj.as<bool>(), result, false);
  case msgpack::type::POSITIVE_INTEGER:
    return TryCast::Operation<uint64_t, T>(obj.as<uint64_t>(), result, false);
  case msgpack::type::NEGATIVE_INTEGER:
    return TryCast::Operation<int64_t, T>(obj.as<int64_t>(), result, false);
  case msgpack::type::FLOAT32:
  case msgpack::type::FLOAT64:
    return TryCast::Operation<double, T>(obj.as<double>(), result, false);
  case msgpack::type::STR:
    return TryCast::Operation<string_t, T>(obj.as<string_t>(), result, false);
  case msgpack::type::BIN:
  case msgpack::type::ARRAY:
  case msgpack::type::MAP:
  case msgpack::type::EXT:
    // TODO: implement
  default:
    throw InternalException("Unknown msgpack type in GetValueNumerical");
  }
}

template <class T>
static inline bool TransformNumerical(msgpack::object *values[], Vector &result,
                                      const idx_t count) {
  auto data = FlatVector::GetData<T>(result);
  auto &validity = FlatVector::Validity(result);
  for (idx_t row_idx = 0; row_idx < count; row_idx++) {
    auto &obj = *values[row_idx];
    if (obj.type == msgpack::type::NIL ||
        !GetValueNumerical(obj, data[row_idx])) {
      validity.SetInvalid(row_idx);
    }
  }
  return true;
}

static inline bool GetValueString(msgpack::object const &obj, string_t &result,
                                  Vector &vector) {
  D_ASSERT(obj.type != msgpack::type::NIL);
  switch (obj.type) {
  case msgpack::type::BOOLEAN:
    result = StringCast::Operation<bool>(obj.as<bool>(), vector);
    return true;
  case msgpack::type::POSITIVE_INTEGER:
    result = StringCast::Operation<uint64_t>(obj.as<uint64_t>(), vector);
    return true;
  case msgpack::type::NEGATIVE_INTEGER:
    result = StringCast::Operation<int64_t>(obj.as<int64_t>(), vector);
    return true;
  case msgpack::type::FLOAT32:
  case msgpack::type::FLOAT64:
    result = StringCast::Operation<double>(obj.as<double>(), vector);
    return true;
  case msgpack::type::STR:
    obj.convert<string_t>(result);
    return true;
  case msgpack::type::BIN:
  case msgpack::type::ARRAY:
  case msgpack::type::MAP:
  case msgpack::type::EXT:
    // TODO: implement
  default:
    throw InternalException("Unknown msgpack type in GetValueString");
  }
}

static inline bool TransformToString(msgpack::object *values[], Vector &result,
                                     const idx_t count) {
  auto data = FlatVector::GetData<string_t>(result);
  auto &validity = FlatVector::Validity(result);
  for (idx_t row_idx = 0; row_idx < count; row_idx++) {
    auto &obj = *values[row_idx];
    if (obj.type == msgpack::type::NIL ||
        !GetValueString(obj, data[row_idx], result)) {
      validity.SetInvalid(row_idx);
    }
  }
  return true;
}

static bool TransformFromString(msgpack::object *values[], Vector &result,
                                const idx_t count) {
  Vector string_vector(LogicalTypeId::VARCHAR, count);
  TransformToString(values, string_vector, count);

  return VectorOperations::DefaultTryCast(string_vector, result, count, nullptr,
                                          true);
}

bool MsgpackTransform(msgpack::object *values[], Vector &result,
                      const idx_t count) {
  auto result_type = result.GetType();
  switch (result_type.id()) {
  case LogicalTypeId::SQLNULL:
    FlatVector::Validity(result).SetAllInvalid(count);
    return true;
  case LogicalTypeId::BOOLEAN:
    return TransformNumerical<bool>(values, result, count);
  case LogicalTypeId::TINYINT:
    return TransformNumerical<int8_t>(values, result, count);
  case LogicalTypeId::SMALLINT:
    return TransformNumerical<int16_t>(values, result, count);
  case LogicalTypeId::INTEGER:
    return TransformNumerical<int32_t>(values, result, count);
  case LogicalTypeId::BIGINT:
    return TransformNumerical<int64_t>(values, result, count);
  case LogicalTypeId::UTINYINT:
    return TransformNumerical<uint8_t>(values, result, count);
  case LogicalTypeId::USMALLINT:
    return TransformNumerical<uint16_t>(values, result, count);
  case LogicalTypeId::UINTEGER:
    return TransformNumerical<uint32_t>(values, result, count);
  case LogicalTypeId::UBIGINT:
    return TransformNumerical<uint64_t>(values, result, count);
  case LogicalTypeId::FLOAT:
    return TransformNumerical<float>(values, result, count);
  case LogicalTypeId::DOUBLE:
    return TransformNumerical<double>(values, result, count);
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
  case LogicalTypeId::VARCHAR:
    return TransformToString(values, result, count);
  default:
    throw NotImplementedException(
        "Cannot read a value of type %s from a msgpack file",
        result_type.ToString());
  }
}
} // namespace duckdb
