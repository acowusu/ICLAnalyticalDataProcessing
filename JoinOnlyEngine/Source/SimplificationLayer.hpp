#pragma once

#include <Algorithm.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <variant>
namespace simplificationLayer {
using namespace boss;
using namespace utilities;
using Value = std::variant<int64_t, double>;
using Column = std::vector<Value>;
using Table = std::vector<Column>;
using Schema = std::vector<std::string>;

class JoinHelper {
private: // state
  std::vector<Table> inputs;
  std::vector<std::pair<size_t, size_t>> joinAttributeIndices;
  Schema mergedSchema;

  std::vector<std::variant<std::vector<int64_t>, std::vector<double_t>>> result;

private: // utility functions
  std::tuple<Schema, Table> toSchemaAndData(ComplexExpression&& e) {
    Schema schema;
    Table table;
    for(auto&& columnExpr : std::move(e).getDynamicArguments()) {
      auto [head, unused_, dynamics, unused2_] =
          boss::get<ComplexExpression>(std::move(columnExpr)).decompose();
      schema.emplace_back(std::move(head).getName());
      auto list = *std::make_move_iterator(dynamics.begin());
      auto& column = table.emplace_back();
      for(auto&& valExpr : boss::get<ComplexExpression>(list).getArguments()) {
        boss::expressions::generic::visit(
            overload([&column](int64_t v) { column.emplace_back(v); },
                     [&column](double_t v) { column.emplace_back(v); },
                     [](auto&&) { throw std::runtime_error("unsupported type"); }),
            std::move(valExpr));
      }
    }
    return {std::move(schema), std::move(table)};
  }

public:
  JoinHelper(const JoinHelper&) = default;
  JoinHelper(JoinHelper&&) = default;
  JoinHelper& operator=(const JoinHelper&) = default;
  JoinHelper& operator=(JoinHelper&&) = default;
  JoinHelper(Expression&& expr) {
    auto [schemas, tables, indices] = getInputsFromPlan(std::move(expr));
    for(auto indicesIt = indices.begin(); indicesIt != indices.end(); indicesIt += 2) {
      joinAttributeIndices.emplace_back(std::move(*indicesIt), std::move(*(indicesIt + 1)));
    }
    inputs = std::move(tables);
    for(auto&& schema : schemas) {
      mergedSchema.insert(mergedSchema.end(), std::make_move_iterator(schema.begin()),
                          std::make_move_iterator(schema.end()));
    }
  }

  std::tuple<std::vector<Schema>, std::vector<Table>, std::vector<size_t>>
  getInputsFromPlan(Expression&& expr) {
    return std::visit(
        overload(
            [this](ComplexExpression&& expression)
                -> std::tuple<std::vector<Schema>, std::vector<Table>, std::vector<size_t>> {
              if(expression.getHead() == "Table"_) {
                auto [schema, table] = toSchemaAndData(std::move(expression));
                return {{std::move(schema)}, {std::move(table)}, {}};
              }
              if(expression.getHead() == "Join"_) {
                auto args = std::move(expression).getDynamicArguments();
                // get data from all the tables in the plan
                auto [schemas, tables, indices] = getInputsFromPlan(std::move(args.at(0)));
                auto [subSchemas, subTables, subIndices] = getInputsFromPlan(std::move(args.at(1)));
                schemas.insert(schemas.end(), subSchemas.begin(), subSchemas.end());
                tables.insert(tables.end(), subTables.begin(), subTables.end());
                // get the indices from this predicate and merge with all other predicates' indices
                auto thisIndices = getIndicesFromPlan(std::move(args.at(2)), schemas);
                indices.insert(indices.end(), subIndices.begin(), subIndices.end());
                indices.insert(indices.end(), thisIndices.begin(), thisIndices.end());
                return {std::move(schemas), std::move(tables), std::move(indices)};
              }
              return {};
            },
            [](auto const& x)
                -> std::tuple<std::vector<Schema>, std::vector<Table>, std::vector<size_t>> {
              return {};
            }),
        std::move(expr));
  }

  std::vector<size_t> getIndicesFromPlan(Expression&& expr, std::vector<Schema> const& schemas) {
    return std::visit(overload(
                          [this, &schemas](ComplexExpression&& expression) -> std::vector<size_t> {
                            if(expression.getHead() == "Equal"_) {
                              // retrieves indices for all the symbols used in the predicates
                              auto args = std::move(expression).getDynamicArguments();
                              auto indices = getIndicesFromPlan(std::move(args.at(0)), schemas);
                              auto subs = getIndicesFromPlan(std::move(args.at(1)), schemas);
                              indices.insert(indices.end(), subs.begin(), subs.end());
                              return indices;
                            }
                            if(expression.getHead() == "Where"_) {
                              auto args = std::move(expression).getDynamicArguments();
                              return getIndicesFromPlan(std::move(args.at(0)), schemas);
                            }
                            return {};
                          },
                          [this, &schemas](Symbol&& s) -> std::vector<size_t> {
                            for(auto const& schema : schemas) {
                              // match a column name (symbol) with a column name in the schema
                              auto findIt = std::find(schema.begin(), schema.end(), s.getName());
                              if(findIt != schema.end()) {
                                return {(size_t)std::distance(schema.begin(), findIt)};
                              }
                            }
                            throw std::runtime_error("unknown column symbol: " + s.getName());
                          },
                          [](auto const& x) -> std::vector<size_t> { return {}; }),
                      std::move(expr));
  }

  std::vector<Table> const& getInputs() { return inputs; }
  std::vector<std::pair<size_t, size_t>> const& getJoinAttributeIndices() {
    return joinAttributeIndices;
  }

  void appendOutput(Table::value_type resultTuple) {
    if(result.size() != resultTuple.size())
      for(auto& it : resultTuple) {
        std::visit(
            [this](auto&& it) { result.emplace_back(std::vector<std::decay_t<decltype(it)>>()); },
            it);
      }
    for(auto i = 0U; i < resultTuple.size(); i++)
      std::visit([i, this](auto value, auto& columnVector) { columnVector.push_back(value); },
                 resultTuple[i], result[i]);
  };

  auto getResult() {
    auto columns = ExpressionArguments();
    auto resultIt = std::move_iterator(result.begin());
    for(auto&& columnName : mergedSchema) {
      ExpressionArguments args;
      if(resultIt != std::move_iterator(result.end())) {
        args.emplace_back(std::visit(
            [&args](auto&& columnVector) {
              return "List"_(Span<typename std::decay_t<decltype(columnVector)>::value_type>(
                  std::move(columnVector)));
            },
            *resultIt++));
      } else {
        args.emplace_back("List"_());
      }
      columns.emplace_back(
          boss::expressions::ComplexExpression(Symbol(std::move(columnName)), std::move(args)));
    }
    return ComplexExpression("Table"_, {}, std::move(columns));
  }
};
} // namespace simplificationLayer
