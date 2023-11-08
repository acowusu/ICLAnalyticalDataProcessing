#include "SortMergeJoinOnly.hpp"
#include "Common.hpp"

#include "SimplificationLayer.hpp"
#include <Algorithm.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <mutex>

namespace boss::engines::joinonly {
using std::vector;

size_t partition(vector<size_t> &vect, int start, int end)
{
  int pivotIndex = start + (end - start) / 2;
  size_t pivot = vect[start + (end - start) / 2];
  int i = start, j = end;
  while (i <= j) {
    // Get swap points
    while (vect[i] < pivot) {
      i++;
    }
    while (vect[j] > pivot) {
      j--;
    }
    if (i <= j) {
      // We need to swap!
      size_t temp = vect[i];
      vect[i] = vect[j];
      vect[j] = temp;
      i++;
      j--;
    }
  }
  return i;
}

void quicksort(vector<size_t> &vect, int start, int end) {
  if (start < end) {
    int pivotIndex = partition(vect, start, end);
    quicksort(vect, start, pivotIndex - 1);
    quicksort(vect, pivotIndex, end);
  }
}

auto sortVectorTable(std::vector<simplificationLayer::Table> input, std::vector<std::pair<size_t, size_t>> joinAttributeIndicies) {
  // Sort n table by n'th join attr's first element, 
  // then sort n+1 table by n'th attr's second element, then repeat
  
  return input;
}

class Engine {
  simplificationLayer::JoinHelper performMultiwayJoin(simplificationLayer::JoinHelper&& helper) {

    ////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////// Your code starts here ////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////

    auto const& input = helper.getInputs();
    auto const& joinAttributeIndices = helper.getJoinAttributeIndices();
    auto cursors = vector<int>(input.size(), 0);

    auto const& sortedInput = sortVectorTable(input, joinAttributeIndices);

    auto const makeCursorsOverflow = [&]() {
      for(auto i = cursors.size() - 1; i > 0; i--) {
        if(cursors[i] >= input[i][0].size()) {
          cursors[i] = 0;
          cursors[i - 1]++;
        }
      }
    };

    for(; cursors[0] < input[0][0].size();) {
      auto isAMatchSoFar = true;
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
      if(isAMatchSoFar) {
        vector<simplificationLayer::Value> resultTuple;
        for(auto i = 0u; i < input.size(); i++) {
          for(auto& column : input[i]) {
            resultTuple.push_back(column[cursors[i]]);
          }
        }
        cursors.back()++;
        helper.appendOutput(resultTuple);
      }
      makeCursorsOverflow();
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
