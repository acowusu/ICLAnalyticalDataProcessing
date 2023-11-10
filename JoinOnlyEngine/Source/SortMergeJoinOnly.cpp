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

// size_t partition(simplificationLayer::Column col, int start, int end)
// {
//   int pivotIndex = start + (end - start) / 2;
//   simplificationLayer::Value pivot = col[start + (end - start) / 2];
//   int i = start, j = end;
//   while (i <= j) {
//     // Get swap points
//     while (col[i] < pivot) {
//       i++;
//     }
//     while (col[j] > pivot) {
//       j--;
//     }
//     if (i <= j) {
//       // We need to swap!
//       simplificationLayer::Value temp = col[i];
//       col[i] = col[j];
//       col[j] = temp;
//       i++;
//       j--;
//     }
//   }
//   return i;
// }

// void quicksort(simplificationLayer::Column &col, int start, int end) {
//   if (start < end) {
//     int pivotIndex = partition(col, start, end);
//     quicksort(col, start, pivotIndex - 1);
//     quicksort(col, pivotIndex, end);
//   }
// }

size_t partition_tab(vector<simplificationLayer::Column> tab, size_t index, int start, int end)
{
  int pivotIndex = start + (end - start) / 2;
  simplificationLayer::Value pivot = tab[index][start + (end - start) / 2];
  int i = start, j = end;
  while (i <= j) {
    // Get swap points
    while (tab[index][i] < pivot) {
      i++;
    }
    while (tab[index][j] > pivot) {
      j--;
    }
     if (i <= j) {
      // We need to swap the rows!
      for (size_t columnIndex = 0; i < tab.size(); i++)
      {
        simplificationLayer::Value temp = tab[columnIndex][i];
        tab[columnIndex][i] = tab[columnIndex][j];
        tab[columnIndex][j] = temp;
      }
      
      
      i++;
      j--;
    }
  }
  return i;
}

void quicksort_tab(vector<simplificationLayer::Column> tab, size_t index, int start, int end) {
  if (start < end) {
    int pivotIndex = partition_tab(tab, index, start, end);
    quicksort_tab(tab, index, start, pivotIndex - 1);
    quicksort_tab(tab, index, pivotIndex, end);
  }
}

auto sortVectorTable(std::vector<simplificationLayer::Table> input, std::vector<std::pair<size_t, size_t>> joinAttributeIndicies) {
  // Sort n table by n'th join attr's first element, 
  // then sort n+1 table by n'th attr's second element, then repeat

  //
  vector<vector<simplificationLayer::Value>> relevantCols;
  for (size_t i = 0; i < joinAttributeIndicies.size(); i++)
  {
    quicksort_tab(input[i], joinAttributeIndicies[i].first, 0, input[i].size());
    quicksort_tab(input[i+1], joinAttributeIndicies[i].second, 0, input[i+1].size());
  }
  
  
  return relevantCols;
}

class Engine {
  simplificationLayer::JoinHelper performMultiwayJoin(simplificationLayer::JoinHelper&& helper) {

    ////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////// Your code starts here ////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////

    auto const& input = helper.getInputs();
    auto const& joinAttributeIndices = helper.getJoinAttributeIndices();
    auto cursors = vector<int>(input.size(), 0);

    sortVectorTable(input, joinAttributeIndices);

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
