#pragma once
#include <string>

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct MsgpackScanData : public TableFunctionData {};

struct MsgpackScanInfo : public TableFunctionInfo {};

struct MsgpackGlobalTableFunctionState : public GlobalTableFunctionState {};

struct MsgpackLocalTableFunctionState : public LocalTableFunctionState {};

class MsgpackExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

} // namespace duckdb
