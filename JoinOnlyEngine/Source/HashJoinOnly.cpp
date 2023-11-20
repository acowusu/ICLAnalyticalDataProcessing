#include "HashJoinOnly.hpp"
#include "Common.hpp"

#include "SimplificationLayer.hpp"
#include <Algorithm.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <mutex>
#include <optional>
#include <vector>

namespace boss::engines::joinonly {
using std::vector;

class Engine {
  simplificationLayer::JoinHelper performMultiwayJoin(simplificationLayer::JoinHelper&& helper) {

    ////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////// Your code starts here ////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////
  //std::vector<Table> const&
    auto const& input = helper.getInputs();
    auto const& joinAttributeIndices = helper.getJoinAttributeIndices();

/**
 * THIS IS CURRENTLY ONLY A HASHJOIN SINGLEWAY.
 *
 * TODO: MAKE MULTIWAY
 */

    std::hash<simplificationLayer::Value> valueHash;
    // Chose first table as build table
    // First in pair is the value, second is index
    vector<std::optional<std::pair<simplificationLayer::Value, size_t>>> hashTable;
    int nextSlot;

    // Build!!
    auto &buildTable = input[0];
    for (int i = 0; i < buildTable[0].size(); i++) {
      auto &buildRow = buildTable[i];

      auto buildValue =buildRow[joinAttributeIndices[0].first];
      int hash = valueHash(buildValue);
      // Handle repeats
      while (hashTable[hash].has_value())
        hash = (hash + 1) % hashTable.size();
      hashTable[hash]->first = buildValue;
      hashTable[hash]->second = i;
    }

    // Join!!
    auto &probeTable = input[1];
    for (int i = 0; i < probeTable[0].size(); i++) {
      auto &probeRow = probeTable[i];
      auto probeInput = probeRow[joinAttributeIndices[0].second];
      int hash = valueHash(probeInput);
      while (hashTable[hash].has_value() && hashTable[hash].value().first != probeInput)
        hash = (hash + 1) % hashTable.size();

      if (hashTable[hash].value().first == probeInput) {
        vector<simplificationLayer::Value> resultTuple;
        for(auto& column : input[0]) {
          // Built table
          resultTuple.push_back(column[hashTable[hash].value().second]);
        }
        for(auto& column : input[1]) {
          // Probe table
          resultTuple.push_back(column[i]);
        }
        // Append
        helper.appendOutput(resultTuple);
      }

    }

    ////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////// Your code ends here /////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////
    return std::move(helper);
  }

public:
  boss::Expression evaluate(Expression&& expr) { // NOLINT
    try {
      return visit(boss::utilities::overload(
                       [this](ComplexExpression&& e) -> Expression {
                         if(e.getHead().getName() == "Join") {
                           return performMultiwayJoin(simplificationLayer::JoinHelper(std::move(e)))
                               .getResult();
                         } else {
                           // evaluate the sub-expression but leave the wrapping expression intact
                           auto [head, unused_, dynamics, spans] = std::move(e).decompose();
                           std::transform(std::make_move_iterator(dynamics.begin()),
                                          std::make_move_iterator(dynamics.end()), dynamics.begin(),
                                          [this](auto&& arg) {
                                            return evaluate(std::forward<decltype(arg)>(arg));
                                          });
                           return boss::ComplexExpression(std::move(head), {}, std::move(dynamics),
                                                          std::move(spans));
                         }
                       },
                       [](auto&& v) -> Expression { return std::move(v); }),
                   std::move(expr));
    } catch(std::exception const& e) {
      ExpressionArguments args;
      args.emplace_back(std::move(expr));
      args.emplace_back(std::string{e.what()});
      return ComplexExpression{boss::Symbol("ErrorWhenEvaluatingExpression"), std::move(args)};
    }
  }
};

} // namespace boss::engines::joinonly

static auto& enginePtr(bool initialise = true) {
  static auto engine = std::unique_ptr<boss::engines::joinonly::Engine>();
  if(!engine && initialise) {
    engine.reset(new boss::engines::joinonly::Engine());
  }
  return engine;
}

extern "C" BOSSExpression* evaluate(BOSSExpression* e) {
  static std::mutex m;
  std::lock_guard lock(m);
  auto* r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
