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

/**
 * @brief Partitions a Table based on a pivot value from a specified column.
 *
 * This function takes a Table and partitions it based on the values in the
 * `sort_index`. It uses the Hoare partition scheme
 * (see https://en.wikipedia.org/wiki/Quicksort#Hoare_partition_scheme),
 * choosing a pivot from the middle of the range. The elements less than the
 * pivot are moved to the left, and those greater are moved to the right.
 *
 * @param table The table to be partitioned must be non const.
 * @param sort_index The index of the column used for partitioning.
 * @param start The starting index of the range to partition (INCLUSIVE).
 * @param end The ending index of the range to partition (INCLUSIVE).
 * @return The final index of the pivot element after partitioning.
 *
 * @note The partition is done in-place, modifying the original vector
 *       so must be mutable.
 * @warning The function assumes that the values in the sort column support
 *          the comparison operators (< and >).
 */
size_t partitionTable(vector<simplificationLayer::Column>& table, size_t sort_index, size_t start,
                      size_t end) {
  std::vector<simplificationLayer::Value> const& pivot_column = table[sort_index];
  size_t COLUMN_COUNT = table.size();
  size_t pivot_index = start + (end - start) / 2;
  simplificationLayer::Value pivot_value = pivot_column[pivot_index];
  size_t i = start - 1;
  size_t j = end + 1;
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
    // Swap rows
    for(size_t columnIndex = 0; columnIndex < COLUMN_COUNT; columnIndex++) {
      simplificationLayer::Value temp = table[columnIndex][i];
      table[columnIndex][i] = table[columnIndex][j];
      table[columnIndex][j] = temp;
    }
  }
}

/**
 * @brief Sorts a vector of columns in ascending order based on a specified column index.
 *
 * This function implements the Quicksort algorithm to sort a table. It
 * recursively partitions the table based on the values in the specified sort_index
 * column. The partitioning is done in-place, modifying the original vector.
 *
 * @param table The table to be sorted.
 * @param sort_index The index of the column used for sorting.
 * @param start The starting index of the range to be sorted.
 * @param end The ending index of the range to be sorted.
 *
 * @note The sorting is done in-place, modifying the original vector.
 * @warning The function assumes that the values in the specified columns support
 *          the comparison operators (< and >).
 * @warning The function does not check if start and end indices are within the valid
 *          range. Specifically to sort a whole vector the end index
 *          should be set to vector.size() - 1.
 */
void quicksortTable(vector<simplificationLayer::Column>& table, size_t sort_index, size_t start,
                    size_t end) {
  if(start >= 0 && end >= 0 && start < end) {
    int pivot_index = partitionTable(table, sort_index, start, end);
    quicksortTable(table, sort_index, start, pivot_index);
    quicksortTable(table, sort_index, pivot_index + 1, end);
  }
}

/**
 * @brief Outputs rows one by one from a table in column-store form.
 *
 * This function takes a JoinHelper object and a table in column-store form and outputs
 * the rows one by one.
 *
 * @param helper The JoinHelper responsible for pushing the data to the next stage in the pipeline.
 * @param result_table The vector of column vectors representing the table in column-store form.
 *
 * @note The function assumes that the input table is well-formed and has consistent column lengths.
 */
inline void outputRows(simplificationLayer::JoinHelper& helper,
                       const vector<vector<simplificationLayer::Value>>& result_table) {
  for(int i = 0; i < result_table[0].size(); ++i) {
    std::__1::vector<simplificationLayer::Value> resultRow;
    for(int j = 0; j < result_table.size(); ++j) {
      resultRow.push_back(result_table[j][i]);
    }
    helper.appendOutput(resultRow);
  }
}

/**
 * @brief Handles a match during a join operation by populating the result table.
 *
 * This function is responsible for handling a match between values in the left and right tables
 * during a join.
 * It keeps track of duplicates in both tables ensuring that the cartesian product is given for
 * the set of rows that match.
 *
 * @param result_table The vector of columns representing the result table.
 * @param left_table The vector of columns representing the left table in column-store form.
 * @param right_table The vector of columns representing the right table.
 * @param left_index The index of the column in the left table used for matching.
 * @param right_index The index of the column in the right table used for matching.
 * @param left_cursor The cursor indicating the current position in the left table.
 * @param right_cursor The cursor indicating the current position in the right table.
 * @param lv The current value in the left table used for matching.
 * @param rv The current value in the right table used for matching.
 *
 * @note The function returns with all matching tuples added and the left and right cursors avanced
 * to the next unique value.
 *
 * @warning The function may return cursors that are out of bounds of the table.
 */
inline void handleJoinMatch(vector<vector<simplificationLayer::Value>>& result_table,
                            vector<vector<std::variant<long, double>>>& left_table,
                            vector<simplificationLayer::Column>& right_table, size_t left_index,
                            size_t right_index, size_t& left_cursor, size_t& right_cursor,
                            simplificationLayer::Value lv, const simplificationLayer::Value rv) {
  size_t old_right_cursor = right_cursor;
  size_t right_duplicate_count = 0;

  while(lv == rv) {
    do {
      for(int j = 0; j < left_table.size(); ++j) {
        result_table[j].push_back(left_table[j][left_cursor]);
      }
      for(int j = 0; j < right_table.size(); ++j) {
        result_table[j + left_table.size()].push_back(right_table[j][right_cursor]);
      }
    } while(right_table[0].size() > ++right_cursor && right_table[right_index][right_cursor] == lv);
    left_cursor++;
    right_duplicate_count = right_cursor - old_right_cursor;
    right_cursor = old_right_cursor;
    if(left_cursor >= left_table[0].size()) {
      break;
    }
    lv = left_table[left_index][left_cursor];
  }
  right_cursor = old_right_cursor + right_duplicate_count;
  // now either they have stopped matching
  // Or we got to the end of the left table -> continue and we will exit the loop
  // both left_cursor and right_cursor should be pointing to the next unique value
  // if it exists
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

      vector<vector<std::variant<long, double>>> left_table = first ? input[0] : builder_table;
      first = false;
      vector<simplificationLayer::Column> right_table = input[i + 1];
      //  build empty results table
      result_table.clear();
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
      // -- Begin while there are still rows to join --
      while(right_cursor < right_table[0].size() && left_cursor < left_table[0].size()) {
        rv = right_table[right_index][right_cursor];
        lv = left_table[left_index][left_cursor];

        while(lv > rv) {
          right_cursor++;
          if(right_cursor >= right_table[0].size()) {
            break;
          }
          rv = right_table[right_index][right_cursor];
        }

        while(rv > lv) {
          left_cursor++;
          if(left_cursor >= left_table[0].size()) {
            break;
          }
          lv = left_table[left_index][left_cursor];
        }
        if(lv == rv) {
          //  We now have to degrade to a nested loop style iteration to cope with duplicates
          handleJoinMatch(result_table, left_table, right_table, left_index, right_index,
                          left_cursor, right_cursor, lv, rv);
        }
      } // -- End while there are still rows to join --

      // The next table will join on the right table. but the right tables collum indexes will be
      // offset by the size of the left table
      column_offset += left_table.size();
    }; // -- End for each join attribute --

    outputRows(helper, result_table);
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
