#include "SortMergeJoinOnly.hpp"
#include "Common.hpp"

#include "SimplificationLayer.hpp"
#include <Algorithm.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <mutex>
#include <vector>
#include <iostream>
#include <variant>

namespace boss::engines::joinonly {
using std::vector;


size_t partitionTable(vector<simplificationLayer::Column> & table, int primary_sort_index, int secondary_sort_index, size_t start, size_t end)
{
  std::vector<simplificationLayer::Value>& pivot_column = table[primary_sort_index];
//  std::vector<simplificationLayer::Value>& secondary_pivot_column = table[secondary_sort_index];

  size_t pivot_index = start + (end - start) / 2;
  simplificationLayer::Value pivot_value = pivot_column[pivot_index];
  size_t i = start, j = end;
  while (1) {
    // Get swap points
    do {
      i++;
    } while (pivot_column[i] < pivot_value);
    do {
      j--;
    } while (pivot_column[j] > pivot_value);
    if(i >= j) {
      return j;
    }
    // If i < j, or i = j and (no secondary index or secondary index value is unsorted)
    // SEGFAULT HERE
//    bool secondary_sort = secondary_sort_index != -1 && secondary_pivot_column[i] > secondary_pivot_column[j];
//    if (i < j || (i <= j && secondary_sort)) {
      // We need to swap the rows!
      for (size_t columnIndex = 0; i < table.size(); i++)
      {
        simplificationLayer::Value temp = table[columnIndex][i];
        table[columnIndex][i] = table[columnIndex][j];
        table[columnIndex][j] = temp;
      }

  }
}

/**
 * If secondary_sort_index is -1, ignore.
*/
// Sorts a table in place in ascending order by primary_sort_index, then secondary_sort_index
void quicksortTable(vector<simplificationLayer::Column>& table, int primary_sort_index, int secondary_sort_index, int start, int end) {
  if (start >=  end || start < 0 || end < 0) {
    return;
  }
  int pivot_index = partitionTable(table, primary_sort_index, secondary_sort_index, start, end);
  quicksortTable(table, primary_sort_index, secondary_sort_index, start, pivot_index);
  quicksortTable(table, primary_sort_index, secondary_sort_index, pivot_index+1, end);

}

  // Sort n table by n'th join attr's first element,
  // then sort n+1 table by n'th attr's second element, then repeat
void sortVectorTable(std::vector<simplificationLayer::Table> input, std::vector<std::pair<size_t, size_t>> join_attribute_indicies) {

  // This is not tested.
//  vector<vector<simplificationLayer::Value>> relevantCols;

  // Initial sort
  simplificationLayer::Table& first_table = input[0];
  quicksortTable(first_table, join_attribute_indicies.front().first, -1, 0, first_table.size()-1);
  

  for (size_t i = 1; i < join_attribute_indicies.size() - 1; i++)
  {
    simplificationLayer::Table& table = input[i];
    // Middle sorts, have a secondary sort index
    quicksortTable(table, join_attribute_indicies[i-1].second, join_attribute_indicies[i].first, 0, table.size()-1);
  }

  // Last sort
  simplificationLayer::Table& last_table = input.back();
  quicksortTable(last_table, join_attribute_indicies.back().second, -1, 0, last_table.size()-1);
  
//  return relevantCols;
}

class Engine {
  simplificationLayer::JoinHelper performMultiwayJoin(simplificationLayer::JoinHelper&& helper) {


    ////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////// Your code starts here ////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////

    auto const& input = helper.getInputs();
    std::vector<std::pair<size_t, size_t>> const& join_attribute_indices = helper.getJoinAttributeIndices();
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
    /*
    We sort the x input tables, then we consider x cursors

    Table 0    Table 1    Table 2
    1           1   1     1
    2           1   2     2
    3           2   3     3
    5           3   4     5
    6           4   5     6
                5   6

    Start at table 0, check table0[cursor[0]][index[0][0]] = table1[cursor[1]][index[0][1]]  
    then              check table1[cursor[1]][index[1][0]] = table2[cursor[2]][index[1][1]] ...etc
    */

    sortVectorTable(input, join_attribute_indices);



    size_t const num_join_attributes = join_attribute_indices.size();
    for (; cursors[0] < input[0][0].size();) {
      auto match = true;
     // -- Begin for each join attribute --
      for ( size_t i = 0; i < num_join_attributes; ) {
        vector<simplificationLayer::Column> const& left_table = input[i];
        vector<simplificationLayer::Column> const& right_table = input[i+1];

        simplificationLayer::Value lv = left_table[join_attribute_indices[i].first][cursors[i]];
        simplificationLayer::Value rv = right_table[join_attribute_indices[i].second][cursors[i+1]];
//        If the RV is too small, increment the right cursor until it is GEQ the LV.
        std::cout << std::get<int64_t>(lv) << " " << std::get<int64_t>(rv) << std::endl;

//        todo: binary search
          while (lv > rv) {
            cursors[i+1]++;
            // if overflow
            if (cursors[i+1] >= right_table.size()) {
              match = false;
              break;
            }
            rv = right_table[cursors[i+1]][join_attribute_indices[i].second];
          }

//        Then if EQ continue to next table
          if (lv == rv) {
//            todo: check if the next row is also equal.
            i++;
            continue;

          }

//        Otherwise we must have a LV that is too small
//        If the LV is too small, there must be no match with the current set of cursors we have been
//        building. There might have been a duplicate key somewhere so we dont want to start from
//        scratch on the first table, but we do want to advace the previous table's cursor.

        if (lv < rv) {

          cursors[i]++;
          // if overflow
          if (cursors[i] >= left_table.size()) {
            match = false;
            break;
          }
        }
        // Match!
      }; // -- End for each join attribute --

//      Getting to here means either
      //      1) we have a match at the location of the current cursors
      //      2) we need to advance the cursors from the first table
      if(match) {
        std::cout << "Match!" << std::endl;
        vector<simplificationLayer::Value> resultTuple;
        for(auto i = 0u; i < input.size(); i++) {
          vector<simplificationLayer::Column> const& table = input[i];
          for(auto& column : table) {
            resultTuple.push_back(column[cursors[i]]);
          }
        }
        cursors.back()++;
        makeCursorsOverflow();
        helper.appendOutput(resultTuple);
      } else {
        // No match
        break;
      }
    };



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
