#include "NestedLoopJoinOnly.hpp"
#include "Common.hpp"

#include "SimplificationLayer.hpp"
#include <Algorithm.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <mutex>

namespace boss::engines::joinonly {
using std::vector;

class Engine {
  simplificationLayer::JoinHelper performMultiwayJoin(simplificationLayer::JoinHelper&& helper) {

    ////////////////////////////////////////////////////////////////////////////////
    ///////////////// this is the code that is relevant to students ////////////////
    ////////////////////////////////////////////////////////////////////////////////

    auto const& input = helper.getInputs();
    auto const& joinAttributeIndices = helper.getJoinAttributeIndices();
    // Set the number of cursors to the number of input tables
    auto cursors = vector<int>(input.size(), 0);

    // Suppose we had cursors [0, 0] and table sizes [2, 2]. Then we would want to increment as
    // follows:
    // [0, 0] -> [0, 0]
    // [0, 1] -> [0, 1]
    // [0, 2] -> [1, 0]
    // [1, 0] -> [1, 0]
    // [1, 2] -> [2, 0]
    // The idea is we maintain the invaruant that all cursors will point to a table value except
    // possibly the first one.
    auto const makeCursorsOverflow = [&]() {
      for(auto i = cursors.size() - 1; i > 0; i--) {
        if(cursors[i] >= input[i][0].size()) {
          cursors[i] = 0;
          cursors[i - 1]++;
        }
      }
    };
//    For each row in the first table
    for(; cursors[0] < input[0][0].size();) {
      auto isAMatchSoFar = true;
//      Check each table to see if the join attribute matches
//      `i`  corresponds to the table index. We start at 1 because we are comparing the second
//      against the first table
//      We alwayse check adjacent tables
//      We check using our current value of `cursors` to see if the join attribute matches
//      If it does not match, we increment the cursor of the table that is not matching and
//      reset all the cursors of the tables that are to the right of the table that is not matching
//      to 0 - as we comparing them against a new value.
      for(auto i = 1U; isAMatchSoFar && i < input.size(); i++) {
        if(input[i - 1][joinAttributeIndices[i - 1].first][cursors[i - 1]] !=
           input[i][joinAttributeIndices[i - 1].second][cursors[i]]) {
          isAMatchSoFar = false;
          cursors[i]++;
          for(auto j = i + 1; j < input.size(); j++) {
            cursors[j] = 0;
          }
        }
      }
//      When looking at the rows from indexed from each cursor we found a match
      if(isAMatchSoFar) {
        vector<simplificationLayer::Value> resultTuple;
//        For each table
        for(auto i = 0u; i < input.size(); i++) {
//          For each column in the table
          for(auto& column : input[i]) {
//            Add a
            resultTuple.push_back(column[cursors[i]]);
          }
        }
//        Increment the cursor of the last table
        cursors.back()++;
//        Output the result we found
        helper.appendOutput(resultTuple);
      }
      makeCursorsOverflow();
    }

    ////////////////////////////////////////////////////////////////////////////////
    /////////////////// end of code that is relevant to students ///////////////////
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
