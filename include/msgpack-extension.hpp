#pragma once
#include <string>

#include "duckdb.hpp"

namespace duckdb {

  class MsgpackExtension : public Extension {
  public:
    void Load(DuckDB &db) override;
    std::string Name() override;
  };

} // namespace duckdb
