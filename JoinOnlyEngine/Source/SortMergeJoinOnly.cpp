#include "SortMergeJoinOnly.hpp"
#include "Common.hpp"

#include "SimplificationLayer.hpp"
#include <Algorithm.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <iostream>
#include <mutex>
#include <variant>
#include <vector>

namespace boss::engines::joinonly {
using std::vector;

size_t partitionTable(vector<simplificationLayer::Column>& table, size_t sort_index, size_t start,
                      size_t end) {
  std::vector<simplificationLayer::Value> const& pivot_column = table[sort_index];

  size_t pivot_index = start + (end - start) / 2;
  simplificationLayer::Value pivot_value = pivot_column[pivot_index];
  size_t i = start, j = end;
  while(1) {
    // Get swap points
    do {
      i++;
    } while(pivot_column[i] < pivot_value);
    do {
      j--;
    } while(pivot_column[j] > pivot_value);
    if(i >= j) {
      return j;
    }
    for(size_t columnIndex = 0; i < table.size(); i++) {
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
void quicksortTable(vector<simplificationLayer::Column>& table, size_t primary_sort_index,
                    size_t start, size_t end) {
  if(start >= end || start < 0 || end < 0) {
    return;
  }
  int pivot_index = partitionTable(table, primary_sort_index, start, end);
  quicksortTable(table, primary_sort_index, start, pivot_index);
  quicksortTable(table, primary_sort_index, pivot_index + 1, end);
}

class Engine {
  simplificationLayer::JoinHelper performMultiwayJoin(simplificationLayer::JoinHelper&& helper) {

    ////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////// Your code starts here ////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////

    auto const& input = helper.getInputs();
    std::vector<std::pair<size_t, size_t>> const& join_attribute_indices =
        helper.getJoinAttributeIndices();

    ////////////////////////////////////////////////////////////////////////////////

    int debug = 0;
    if(debug == 1) {
      std::cout << "Sorted Tables" << std::endl;
      std::cout << "Count: " << input.size() << std::endl;
      for(int i = 0; i < input.size(); i++) {
        std::cout << "Table " << i << std::endl;
        for(int j = 0; j < input[i][0].size(); j++) {
          for(int k = 0; k < input[i].size(); ++k) {
            std::cout << std::get<int64_t>(input[i][k][j])  << " ";
          }
          std::cout << std::endl;
        }
      }
      std::cout << "Join Attribute Indices" << std::endl;
      for(int i = 0; i < join_attribute_indices.size(); i++) {
        std::cout << join_attribute_indices[i].first << " " << join_attribute_indices[i].second
                  << std::endl;
      }
      std::cout << "END DEBUG" << std::endl;
    }

    size_t const num_join_attributes = join_attribute_indices.size();

    // How much to offset the Left table (since it will contain columns which it has already been
    // joined on)
    size_t column_offset = 0;
    //    Stores the results of the join
    std::vector<vector<simplificationLayer::Value>> builder_table;
    std::vector<vector<simplificationLayer::Value>> result_table;
    bool first = true;
    // -- Begin for each join attribute --
    for(size_t i = 0; i < num_join_attributes; i++) {
      size_t left_cursor = 0;
      size_t right_cursor = 0;
      builder_table = result_table;
      result_table.clear();

      vector<vector<std::variant<long, double>>> left_table = first ? input[0] : builder_table;
      first = false;
      vector<simplificationLayer::Column>& right_table =
          const_cast<vector<simplificationLayer::Column>&>(input[i + 1]);
      //      build empty results table
      for(int j = 0; j < left_table.size() + right_table.size(); j++) {
        result_table.push_back(vector<simplificationLayer::Value>());
      }
      std::pair<size_t, size_t> const& join_attribute_index = join_attribute_indices[i];
      size_t left_index = join_attribute_index.first + column_offset;
      size_t right_index = join_attribute_index.second; // no offset needed
      //        Sort both tables by the join attribute
      quicksortTable(left_table, left_index, 0, left_table[0].size() - 1);
      quicksortTable(right_table, right_index, 0, right_table[0].size() - 1);

      simplificationLayer::Value lv = left_table[left_index][left_cursor];
      simplificationLayer::Value rv = right_table[right_index][right_cursor];

      //      Before we start lets print the tables
//      std::cout << "Left table  " << left_table.size() << "x" << left_table[0].size() << std::endl;

      if(debug==1){
        for(int i = 0; i < left_table[0].size(); i++) {

          for(int j = 0; j < left_table.size(); j++) {
            if(j == left_index) {
              std::cout << "=";
            }
            std::cout << std::get<int64_t>(left_table[j][i]) << ", ";
          }
          std::cout << std::endl;
        }
        std::cout << "Right table  " << right_table.size() << "x" << right_table[0].size()
                  << std::endl;
        for(int i = 0; i < right_table[0].size(); i++) {
          for(int j = 0; j < right_table.size(); j++) {
            if(j == right_index) {
              std::cout << "=";
            }
            std::cout << std::get<int64_t>(right_table[j][i]) << ", ";
          }
          std::cout << std::endl;
        }
      }

      while(right_cursor < right_table[0].size() && left_cursor < left_table[0].size()) {
        rv = right_table[right_index][right_cursor];
        lv = left_table[left_index][left_cursor];

        while(lv > rv) {
          if(debug == 1) {
            std::cout << "LV:" << std::get<int64_t>(lv) << " > "
                      << "RV:" << std::get<int64_t>(rv) << std::endl;
          }
          right_cursor++;
          // if overflow
          if(right_cursor >= right_table[0].size()) {
            break;
          }
          rv = right_table[right_index][right_cursor];
        }

        while(rv > lv) {
          if(debug == 1) {
            std::cout << "LV:" << std::get<int64_t>(lv) << " < "
                      << "RV:" << std::get<int64_t>(rv) << std::endl;
          }
          left_cursor++;
          // if overflow
          if(left_cursor >= left_table[0].size()) {
            break;
          }
          lv = left_table[left_index][left_cursor];
        }
        //        Then if EQ continue to next table
        if(lv == rv) {

          size_t old_right_cursor = right_cursor;
          size_t right_duplicate_count = 0;
          //            We now have to degrade to a nested loop style iteration to cope with
          //            duplicates
          while(lv == rv) {
            do {
              if(debug == 1) {
                std::cout << "MATCH <LV:" << std::get<int64_t>(lv) << " = "
                          << "RV:" << std::get<int64_t>(rv) << "> ";
              }
              for(int j = 0; j < left_table.size(); ++j) {
                result_table[j].push_back(left_table[j][left_cursor]);
                if(debug==1){
                  std::cout << std::get<int64_t>(left_table[j][left_cursor]) << " ";
                }
              }
              if(debug==1){
                std::cout << "| ";
              }
              for(int j = 0; j < right_table.size(); ++j) {
                result_table[j + left_table.size()].push_back(right_table[j][right_cursor]);
                if(debug==1){
                  std::cout << std::get<int64_t>(right_table[j][right_cursor]) << " ";
                }
              }
              if(debug==1){
                std::cout << std::endl;
              }
            } while(right_table[0].size() > ++right_cursor &&
                    right_table[right_index][right_cursor] == lv);
            left_cursor++;
            right_duplicate_count = right_cursor - old_right_cursor;
            right_cursor = old_right_cursor;
            if(left_cursor >= left_table[0].size()) {
              break;
            }
            lv = left_table[left_index][left_cursor];
          }
          right_cursor = old_right_cursor + right_duplicate_count;
          //          now ether they have stopped matching in which case LV is 1 ahead.
          //          Or we got to the end of the left table -> continue and we will exit the loop
          //          RV should
          //          both left_cursor and right_cursor should be pointing to the next unique value
          //          if it exists
        }
      }
      //      result table must have added all the colums from the right table
      column_offset += left_table.size();
    }; // -- End for each join attribute --

    //    OUTPUt as rows
    if(debug == 1) {
      std::cout << "Results:" << result_table.size() << " by" << result_table[0].size()
                << std::endl;
    }
    for(int i = 0; i < result_table[0].size(); ++i) {
      vector<simplificationLayer::Value> resultRow;
      for(int j = 0; j < result_table.size(); ++j) {
        resultRow.push_back(result_table[j][i]);
        if(debug == 1) {
          std::cout << std::get<int64_t>(result_table[j][i]) << " ";
        }
      }
      helper.appendOutput(resultRow);
      if(debug == 1) {
        std::cout << std::endl;
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
