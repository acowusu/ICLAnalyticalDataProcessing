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

    auto const& input = helper.getInputs();
    auto const& joinAttributeIndices = helper.getJoinAttributeIndices();

    std::hash<simplificationLayer::Value> const valueHash;
    // First in pair is the value, second is vector of index
    vector<vector<std::pair<std::optional<simplificationLayer::Value>, vector<size_t>>>> hashTables(
      joinAttributeIndices.size());
    int nextSlot;

    // First, we Build!!
    for (int c = 0; c < joinAttributeIndices.size(); c++) {
      hashTables[c] = vector<std::pair<std::optional<simplificationLayer::Value>, vector<size_t>>>(
        input[c][0].size() * 2);
      for (int i = 0; i < input[c][0].size(); i++) {

        auto buildValue = input[c][joinAttributeIndices[c].first][i];
        size_t hash = valueHash(buildValue) % hashTables[c].size();
        // Handle repeats
        while (hashTables[c][hash].first.has_value() && hashTables[c][hash].first.value() != buildValue)
          hash = (hash + 1) % hashTables[c].size();
        hashTables[c][hash].first = buildValue;
        hashTables[c][hash].second.push_back(i);
        if (hashTables[c][hash].second.size() > 1)
        {
          // We have a repeat!
          int unusedvar = 0; // used for testing
        }
      }
    }

    auto &probeTable = input[input.size()-1];
    for (int i = 0; i < probeTable[0].size(); i++)
    {
      // Each row in the probe table

      /**
       * This data object contains the values of the join.
       * [[[4,1,0],[3,2,0]], [[1,0],[2,0]], [[0]]]
       * Implies that we have a join on cursors 4,1,0 and 3,2,0
       */
      vector<vector<vector<size_t>>> indexes(input.size(), vector<vector<size_t>>(0, vector<size_t>(0))); // Indexes of the join
      indexes[input.size()-1].emplace_back(0);
      indexes[input.size()-1][0].push_back(i); // Index of last table is i

      for (int j = hashTables.size() - 1; j >= 0; j--) {
        // For each hashtable, backwards
        vector<int> curIndexes;
        for (int k = 0; k < indexes[j+1].size(); k++) {
          // For each index of the hashtable ahead
          vector<size_t> index = indexes[j+1][k];
          auto val = input[j+1][joinAttributeIndices[j].second][index[0]];
          int hash = valueHash(val) % hashTables[j].size();

          // handle clashes
          while (hashTables[j][hash].first.has_value() && hashTables[j][hash].first.value() != val)
            hash = (hash + 1) % hashTables[j].size();
        
          if (hashTables[j][hash].first.has_value() && hashTables[j][hash].first.value() == val) {
            // Add to indexes
            for (int l = 0; l < hashTables[j][hash].second.size(); l++) {
              // This new index is the details of the last index, with the addition of the current
              // found mapping. This is O(n), and can be inefficient with longer join chains.
              // We have opted for code readability instead of this slight code improvement, meaning
              // we dont have to work backwards.
              // In addition, some compilers on some architectures can optimise this, to have the vector
              // point to the next vector.
              vector<size_t> newIndex;
              newIndex.push_back(hashTables[j][hash].second[l]);
              for (auto ind: index)
              {
                newIndex.push_back(ind);
              }

              indexes[j].push_back(newIndex);
            }
          }
        }
      }

      // Return result of indexes!

      /**
       * Implimentation detail:
       * This can be moved to inside of the probing function, on the last backward
       * join attribute iteration. This also includes making the indexes object only have a size of 2;
       * [0] being the current iteration, and
       * [1] being the iteration before.
       *
       * However, the increased memory cost means we dont have to do memory moving, and we obtain
       * data that can be used to optimise further queries in a more complex system.
       *
       * Lastly, this provides a seperation for more fine grained threading details, if the need arises.
       */
      for (auto ind: indexes[0])
      {
        // populate resultTuple
        vector<simplificationLayer::Value> resultTuple;

        for (int j = 0; j < ind.size(); j++) {
          // for each index
          // for each col in table
          for (int k = 0; k < input[j].size(); k++) {
            // push value
            resultTuple.push_back(input[j][k][ind[j]]);
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
