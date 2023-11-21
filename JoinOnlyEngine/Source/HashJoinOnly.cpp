#include "HashJoinOnly.hpp"
#include "Common.hpp"

#include "SimplificationLayer.hpp"
#include <Algorithm.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <iostream>
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
    /// 1 3 - 3 4  32 - 4 1
    /// 2 4 - 4 7  94 - 7 2
    ///     - 6 10 77
    ///
    /// 1 3 - 3 4 - 1 4
    /// 2 4 - 4 7 - 2 7
    ///     - 6 10
    /// join attr 1,0 1,1
    ///
    /// indexes:
    /// [0, 0, 0]
    /// [1, 1, 1]
    ///
    /// instead gets [0,0,0]
    /// [1,1,1]
    ///
  //std::vector<Table> const&
    auto const& input = helper.getInputs();
    auto const& joinAttributeIndices = helper.getJoinAttributeIndices();


    vector<vector<vector<int64_t>>> inputs(input.size(), vector<vector<int64_t>>(100, vector<int64_t>(100)));

    for(int j = 0; j < input.size(); j++) {
      for(int k = 0; k < input[j].size(); k++)
      {
        for(int l = 0; l < input[j][k].size(); l++)
        {
          inputs[j][k][l] = std::get<int64_t>(input[j][k][l]);
        }
      }
    }
/**
 * THIS IS CURRENTLY ONLY A HASHJOIN SINGLEWAY.
 *
 * TODO: MAKE MULTIWAY
 */

    int const HASH_TABLE_SIZE = 1024 * 256;

    std::hash<simplificationLayer::Value> const valueHash;
    // Chose first table as build table
    // First in pair is the value, second is vector of index
    vector<vector<std::pair<std::optional<simplificationLayer::Value>, vector<size_t>>>> hashTables(
      joinAttributeIndices.size(),
      vector<std::pair<std::optional<simplificationLayer::Value>, vector<size_t>>>(HASH_TABLE_SIZE));
    int nextSlot;

    // Build!!
    for (int c = 0; c < joinAttributeIndices.size(); c++) {
      for (int i = 0; i < input[c][0].size(); i++) {

        auto buildValue = input[c][joinAttributeIndices[c].first][i];
        size_t hash = valueHash(buildValue) % hashTables[c].size();
        // Handle repeats
        while (hashTables[c][hash].first.has_value())
          hash = (hash + 1) % hashTables[c].size();
        hashTables[c][hash].first = buildValue;
        hashTables[c][hash].second.push_back(i);
      }
    }

    auto &probeTable = input[input.size()-1];
    for (int i = 0; i < probeTable[0].size(); i++) {
      // Each row in the probe table

      vector<vector<size_t>> indexes(input.size(), vector<size_t>(0, 0)); // Indexes of the join
      indexes[input.size()-1].push_back(i); // Index of last table is i

      for (int j = hashTables.size() - 1; j >= 0; j--) {
        // For each hashtable, backwards
        vector<int> curIndexes;
        for (int k = 0; k < indexes[j+1].size(); k++) {
          // For each index of the hashtable ahead
          int index = indexes[j+1][k];
          auto val = input[j+1][joinAttributeIndices[j].second][index];
          int hash = valueHash(val) % hashTables[j].size();

          // handle clashes
          while (hashTables[j][hash].first.has_value() && hashTables[j][hash].first.value() != val)
          hash = (hash + 1) % hashTables[j].size();
        
          if (hashTables[j][hash].first.has_value() && hashTables[j][hash].first.value() == val) {
            // Add to indexes
            for (int l = 0; l < hashTables[j][hash].second.size(); l++) {
              indexes[j].push_back(hashTables[j][hash].second[l]);
            }
          }
        }
      }

      // Return result of indexes!



      vector<int> cursors(indexes.size());
      auto num = 1; // Total number of additions
      // Check for initial overflow
      for (int j = 0; j < cursors.size(); j++) {
        num *= indexes[j].size();
      }

      for (int j = 0; j < num; j++) {
        // get cursor nums from total num
        cursors[0] = indexes[0].size();
        for (int k = 1; k < cursors.size(); k++) {
          cursors[k] = cursors[k-1] % indexes[k].size();
        }

        // populate resultTuple
        vector<simplificationLayer::Value> resultTuple;

        for (int j = 0; j < cursors.size(); j++) {
          // for each cursor
          // for each col in table
          for (int k = 0; k < input[j].size(); k++) {
            // push value
            resultTuple.push_back(input[j][k][cursors[j]]);
          }
        }

        // Append!
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
