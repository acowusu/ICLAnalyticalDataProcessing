#include "BOSSRemoteBinaryLoaderEngine.hpp"
#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string.h>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#ifdef _WIN32
// curl includes windows headers without using NOMINMAX (causing issues with std::min/max)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif //_WIN32
#include <curl/curl.h>

// #define DEBUG

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss::engines::RBL {
using std::move;

class Engine : public boss::Engine {
private:
  class EngineImplementation& impl;
  friend class EngineImplementation;

public:
  Engine(Engine&) = delete;
  Engine& operator=(Engine&) = delete;
  Engine(Engine&&) = default;
  Engine& operator=(Engine&&) = delete;
  Engine();
  boss::Expression evaluate(Expression&& e);
  ~Engine();
};
} // namespace boss::engines::RBL

namespace boss::engines::RBL {

namespace utilities {
/*
 * In the Expression API, we support only two cases at the moment:
 *   - moving the expressions (which will move the spans' data as well)
 *   - cloning the expressions (which will copy the spans' data)
 * However, when injecting the stored Columns into the query,
 * we want to copy the expression by without moving the spans' data:
 * this is the purpose of shallowCopy().
 *
 * We assume that spans' data will be used only during the storage engine's lifetime,
 * so the spans are still owned by the storage engine.
 */
static boss::ComplexExpression shallowCopy(boss::ComplexExpression const& e) {
  auto const& head = e.getHead();
  auto const& dynamics = e.getDynamicArguments();
  auto const& spans = e.getSpanArguments();
  boss::ExpressionArguments dynamicsCopy;
  std::transform(dynamics.begin(), dynamics.end(), std::back_inserter(dynamicsCopy),
                 [](auto const& arg) {
                   return std::visit(
                       boss::utilities::overload(
                           [&](boss::ComplexExpression const& expr) -> boss::Expression {
                             return shallowCopy(expr);
                           },
                           [](auto const& otherTypes) -> boss::Expression { return otherTypes; }),
                       arg);
                 });
  boss::expressions::ExpressionSpanArguments spansCopy;
  std::transform(spans.begin(), spans.end(), std::back_inserter(spansCopy), [](auto const& span) {
    return std::visit(
        [](auto const& typedSpan) -> boss::expressions::ExpressionSpanArgument {
          // just do a shallow copy of the span
          // the storage's span keeps the ownership
          // (since the storage will be alive until the query finishes)
          using SpanType = std::decay_t<decltype(typedSpan)>;
          using T = std::remove_const_t<typename SpanType::element_type>;
          if constexpr(std::is_same_v<T, bool>) {
            // TODO: this would still keep const spans for bools, need to fix later
            return SpanType(typedSpan.begin(), typedSpan.size(), []() {});
          } else {
            // force non-const value for now (otherwise expressions cannot be moved)
            auto* ptr = const_cast<T*>(typedSpan.begin()); // NOLINT
            return boss::Span<T>(ptr, typedSpan.size(), []() {});
          }
        },
        span);
  });
  return boss::ComplexExpression(head, {}, std::move(dynamicsCopy), std::move(spansCopy));
}

struct ResponseArgs {
  int numCols;
  int currCol;
  size_t rowsLimit;
  std::vector<char> dataBuffer;
  std::vector<std::vector<char>> dataColsBuffer;
};

struct PersistentlyMemoizedCallbackHelper {
  size_t (*writeResponseFunc)(void*, size_t, size_t, void*);
  void* originalCallbackPayload;
  std::string localCacheFilename;
  static size_t memoizer(void* contents, size_t size, size_t nmemb, void* userp) {
    auto helper = static_cast<PersistentlyMemoizedCallbackHelper*>(userp);
    auto localCacheFile =
        std::ofstream(helper->localCacheFilename, std::ios::binary | std::ios::app);
    auto size2 = localCacheFile.tellp();
    localCacheFile.write(static_cast<char*>(contents), nmemb);
    localCacheFile.close();
    return helper->writeResponseFunc(contents, size, nmemb, helper->originalCallbackPayload);
  }
};

} // namespace utilities

struct EngineImplementation {

  using OSMValue = std::variant<int64_t, double>;
  using OSMCol = std::variant<std::vector<int64_t>, std::vector<double>>;

  constexpr static char const* const DefaultNamespace = "BOSS`";
  constexpr static char const COLUMN_DELIMITER = ',';
  boss::Symbol const NO_CURR_TABLE = "NO_CURR_TABLE"_;
  boss::Symbol const TABLE_ALREADY_MEMOISED = "TABLE_ALREADY_MEMOISED"_;
  boss::Symbol const TABLE_MEMOISED = "TABLE_MEMOISED"_;
  struct utilities::ResponseArgs response;
  utilities::PersistentlyMemoizedCallbackHelper persistenceHelper;
  std::vector<boss::Symbol> colSymbols;
  std::vector<std::string> colURLs;
  size_t rowsLimit;
  boss::Symbol currTable = NO_CURR_TABLE;

  std::unordered_map<boss::Symbol, std::unordered_map<boss::Symbol, OSMCol>> columnsInTable;
  std::unordered_map<boss::Symbol, size_t> tableMaxTuples;

  static size_t writeResponseData(void* contents, size_t size, size_t nmemb, void* userp) {
    struct utilities::ResponseArgs* args = (struct utilities::ResponseArgs*)userp;
    size_t size_b = size * nmemb;
    size_t limit_b = args->rowsLimit * args->numCols * sizeof(int64_t);
    size_t len_b = args->dataBuffer.size();

    size_t write_b = std::min(size_b, limit_b - len_b);
    char* byte_contents = reinterpret_cast<char*>(contents);

#ifdef DEBUG
    std::cout << "size: " << len_b << std::endl;
    std::cout << "limit: " << limit_b << std::endl;
    std::cout << "total download b: " << size_b << std::endl;
    std::cout << "writing: " << write_b << std::endl;
#endif

    args->dataBuffer.insert(args->dataBuffer.end(), byte_contents, byte_contents + write_b);
    return write_b;
  };

  static size_t writeResponseDataByCol(void* contents, size_t size, size_t nmemb, void* userp) {
    struct utilities::ResponseArgs* args = (struct utilities::ResponseArgs*)userp;
    size_t size_b = size * nmemb;
    size_t limit_b = args->rowsLimit * sizeof(int64_t);
    size_t len_b = args->dataColsBuffer[args->currCol].size();

    size_t write_b = std::min(size_b, limit_b - len_b);
    char* byte_contents = reinterpret_cast<char*>(contents);

#ifdef DEBUG
    std::cout << "size: " << len_b << std::endl;
    std::cout << "limit: " << limit_b << std::endl;
    std::cout << "total download b: " << size_b << std::endl;
    std::cout << "writing: " << write_b << std::endl;
#endif

    args->dataColsBuffer[args->currCol].insert(args->dataColsBuffer[args->currCol].end(),
                                               byte_contents, byte_contents + write_b);
    return write_b;
  };

  static void
  populateResponseBuffer(struct utilities::ResponseArgs* respArgs,
                         utilities::PersistentlyMemoizedCallbackHelper& persistenceHelper,
                         const size_t limit, const std::string url,
                         size_t (*writeResponseFunc)(void*, size_t, size_t, void*)) {
    respArgs->rowsLimit = limit;
    auto const localCacheFilename =
        std::to_string(std::hash<std::string>()(url)) + "_sf" + std::to_string(limit) + ".bin";
    if(auto localCacheFile = std::ifstream(localCacheFilename, std::ios::binary | std::ios::ate)) {
      auto size = localCacheFile.tellg();
      auto buffer = (char*)malloc(size);
      localCacheFile.read(buffer, size);
      writeResponseFunc(buffer, 1, size, respArgs);
      free(buffer);
      localCacheFile.close();
      return;
    }

    CURL* curl;
    CURLcode res;

    curl = curl_easy_init();
    if(curl) {
#ifdef DEBUG
      char errbuf[CURL_ERROR_SIZE];
      curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
      errbuf[0] = 0;
      curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
#endif

      curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                       utilities::PersistentlyMemoizedCallbackHelper::memoizer);
      persistenceHelper.writeResponseFunc = writeResponseFunc;
      persistenceHelper.originalCallbackPayload = respArgs;
      persistenceHelper.localCacheFilename = localCacheFilename;
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &persistenceHelper);
      res = curl_easy_perform(curl);
      curl_easy_cleanup(curl);

#ifdef DEBUG
      if(res != CURLE_OK) {
        std::cout << "ERROR CODE:" << std::endl;
        std::cout << res << std::endl;
        std::cout << "ERROR BUFFER:" << std::endl;
        std::cout << errbuf << std::endl;
      }
#endif
    }
  };

  void bossExprToRBLLoad(Expression&& expression) {
    std::visit(boss::utilities::overload([&](char const* a) { colURLs.push_back(a); },
                                         [&](std::string&& a) { colURLs.push_back(a); },
                                         [&](Symbol&& a) {
                                           // implement name map for colURLs
                                           // but also for scale factors\limits -- differentiate by
                                           // type
                                         },
                                         [&](int64_t&& a) { rowsLimit = (size_t)a; },
                                         [&](ComplexExpression&& expression) {
                                           auto [head, unused_, dynamics, spans] =
                                               std::move(expression).decompose();
                                           colSymbols.push_back(head);
#ifdef DEBUG
                                           std::cout << "arg head: " << head.getName() << std::endl;
#endif
                                           for(auto it = std::move_iterator(dynamics.begin());
                                               it != std::move_iterator(dynamics.end()); ++it) {

#ifdef DEBUG
                                             std::cout << "argument " << arg << std::endl;
#endif
                                             bossExprToRBLLoad(*it);
                                           }
                                         },
                                         [](auto a) {
                                           std::cerr << "type found: " << typeid(a).name()
                                                     << std::endl;
                                           throw std::runtime_error("unexpected argument type");
                                         }),
               std::move(expression));
  }

  template <typename T> std::vector<T> getColumnData(int colNum) {
    std::vector<T> res;
    size_t typeSize = sizeof(T);
    for(int j = 0; j < response.dataColsBuffer[colNum].size(); j += typeSize) {
      T tmp = 0.0;
      std::memcpy(&tmp, &(response.dataColsBuffer[colNum][j]), typeSize);
      res.push_back(tmp);
    }
    return res;
  }

  EngineImplementation() {}

  EngineImplementation(EngineImplementation&&) = default;
  EngineImplementation(EngineImplementation const&) = delete;
  EngineImplementation& operator=(EngineImplementation&&) = delete;
  EngineImplementation& operator=(EngineImplementation const&) = delete;

  ~EngineImplementation() {}

  boss::Expression evaluate(Expression&& e,
                            std::string const& namespaceIdentifier = DefaultNamespace) {
    return std::visit(
        boss::utilities::overload(
            [this](ComplexExpression&& expression) -> boss::Expression {
              auto [head, unused_, dynamics, spans] = std::move(expression).decompose();
              if(head == "Load"_) {

                auto requestedTupleCount = get<int64_t>(dynamics.front());
                if(tableMaxTuples.find(currTable) != tableMaxTuples.end()) {

                  auto memoisedTupleCount = tableMaxTuples.find(currTable)->second;
                  if(requestedTupleCount <= memoisedTupleCount) {
                    return TABLE_ALREADY_MEMOISED;
                  }
                }
                tableMaxTuples[currTable] = requestedTupleCount;

                for(auto it = std::move_iterator(dynamics.begin());
                    it != std::move_iterator(dynamics.end()); ++it) {
                  bossExprToRBLLoad(*it);
                }
                response.numCols = colSymbols.size();

                for(int i = 0; i < response.numCols; ++i) {
                  response.currCol = i;
                  std::vector<char> buf;
                  response.dataColsBuffer.push_back(buf);
                  populateResponseBuffer(&response, persistenceHelper, rowsLimit, colURLs[i],
                                         &writeResponseDataByCol);
                }
                // perhaps just read all into one buffer but with sep calls -- would need to know
                // prior types tho

                auto beginIDs = getColumnData<int64_t>(0);
                auto endIDs = getColumnData<int64_t>(1);
                auto lens = getColumnData<double>(2);

                // no table symbol to set
                if(currTable == NO_CURR_TABLE) {
                  ExpressionArguments beginIDArgs;
                  beginIDArgs.emplace_back("List"_(boss::Span<int64_t>(std::move(beginIDs))));
                  ExpressionArguments endIDArgs;
                  endIDArgs.emplace_back("List"_(boss::Span<int64_t>(std::move(endIDs))));
                  ExpressionArguments lenArgs;
                  lenArgs.emplace_back("List"_(boss::Span<double>(std::move(lens))));

                  return "Table"_(ComplexExpression(colSymbols[0], std::move(beginIDArgs)),
                                  ComplexExpression(colSymbols[1], std::move(endIDArgs)),
                                  ComplexExpression(colSymbols[2], std::move(lenArgs)));
                }

                // table symbol not set before
                if(columnsInTable.find(currTable) == columnsInTable.end()) {
                  std::unordered_map<boss::Symbol, OSMCol> cols;
                  cols.emplace(std::make_pair(colSymbols[0], beginIDs));
                  cols.emplace(std::make_pair(colSymbols[1], endIDs));
                  cols.emplace(std::make_pair(colSymbols[2], lens));

                  columnsInTable.emplace(std::make_pair(currTable, cols));
                } else {
                  // table symbol set before
                  std::unordered_map<boss::Symbol, OSMCol> cols;
                  cols.emplace(std::make_pair(colSymbols[0], beginIDs));
                  cols.emplace(std::make_pair(colSymbols[1], endIDs));
                  cols.emplace(std::make_pair(colSymbols[2], lens));

                  auto it = columnsInTable.find(currTable);
                  it->second = cols;
                }

                return TABLE_MEMOISED;

              } else if(head == "Set"_) {

                auto tableSymbol = get<boss::Symbol>(dynamics.front());
                auto tableSymbolCopy = get<boss::Symbol>(dynamics.front());
                currTable = tableSymbol;
                auto table = evaluate(std::move(*std::next(dynamics.begin())));
                currTable = NO_CURR_TABLE;
                if(table == TABLE_MEMOISED || table == TABLE_ALREADY_MEMOISED) {
                  return true;
                }

                return false;

              } else if(head == "As"_ &&
                        std::holds_alternative<boss::Symbol>(*std::next(dynamics.begin()))) {

                auto aliasSymbol = get<boss::Symbol>(dynamics.front());
                auto colSymbol = get<boss::Symbol>(*std::next(dynamics.begin()));

                if(columnsInTable.find(currTable) != columnsInTable.end()) {
                  auto& columns = columnsInTable.find(currTable)->second;

                  if(columns.find(colSymbol) != columns.end()) {
                    ExpressionArguments colArgs;
                    if(colSymbol == "length"_) {
                      colArgs.emplace_back("List"_(boss::Span<double>(
                          std::get<std::vector<double>>(columns.find(colSymbol)->second))));
                    } else {
                      colArgs.emplace_back("List"_(boss::Span<int64_t>(
                          std::get<std::vector<int64_t>>(columns.find(colSymbol)->second))));
                    }
                    return ComplexExpression(aliasSymbol, std::move(colArgs));
                  }
                }

                std::transform(
                    std::make_move_iterator(dynamics.begin()),
                    std::make_move_iterator(dynamics.end()), dynamics.begin(),
                    [this](auto&& arg) { return evaluate(std::forward<decltype(arg)>(arg)); });
                return boss::ComplexExpression(std::move(head), {}, std::move(dynamics),
                                               std::move(spans));

              } else if(head == "Project"_ &&
                        std::holds_alternative<boss::Symbol>(dynamics.front())) {

                auto tableSymbol = get<boss::Symbol>(dynamics.front());
                auto tableIt = columnsInTable.find(tableSymbol);
                if(tableIt == columnsInTable.end()) {
                  std::transform(
                      std::make_move_iterator(dynamics.begin()),
                      std::make_move_iterator(dynamics.end()), dynamics.begin(),
                      [this](auto&& arg) { return evaluate(std::forward<decltype(arg)>(arg)); });
                  return boss::ComplexExpression(std::move(head), {}, std::move(dynamics),
                                                 std::move(spans));
                }

                ExpressionArguments tableArgs;
                for(auto it = std::next(std::move_iterator(dynamics.begin()));
                    it != std::move_iterator(dynamics.end()); ++it) {
                  currTable = tableSymbol;
                  tableArgs.emplace_back(evaluate(*it));
                  currTable = NO_CURR_TABLE;
                }

                return ComplexExpression("Table"_, std::move(tableArgs));

              } else {
                std::transform(
                    std::make_move_iterator(dynamics.begin()),
                    std::make_move_iterator(dynamics.end()), dynamics.begin(),
                    [this](auto&& arg) { return evaluate(std::forward<decltype(arg)>(arg)); });
                return boss::ComplexExpression(std::move(head), {}, std::move(dynamics),
                                               std::move(spans));
              }
            },
            [this](Symbol&& symbol) -> boss::Expression {
              auto it = columnsInTable.find(symbol);
              if(it == columnsInTable.end()) {
                return std::move(symbol);
              }
              std::unordered_map<boss::Symbol, OSMCol>& columns =
                  columnsInTable.find(symbol)->second;
              ExpressionArguments tableArgs;
              for(auto colIt = columns.begin(); colIt != columns.end(); ++colIt) {
                ExpressionArguments colArgs;
                if(colIt->first == "length"_) {
                  colArgs.emplace_back(
                      "List"_(boss::Span<double>(std::get<std::vector<double>>(colIt->second))));
                } else {
                  colArgs.emplace_back(
                      "List"_(boss::Span<int64_t>(std::get<std::vector<int64_t>>(colIt->second))));
                }
                tableArgs.emplace_back(ComplexExpression(colIt->first, std::move(colArgs)));
              }

              return ComplexExpression("Table"_, std::move(tableArgs));
            },
            [](auto&& arg) -> boss::Expression { return std::forward<decltype(arg)>(arg); }),
        std::move(e));
  }
};

Engine::Engine()
    : impl([]() -> EngineImplementation& { return *(new EngineImplementation()); }()) {}
Engine::~Engine() { delete &impl; }

boss::Expression Engine::evaluate(Expression&& e) { return impl.evaluate(std::move(e)); };
} // namespace boss::engines::RBL

static auto& enginePtr(bool initialise = true) {
  static auto engine = std::unique_ptr<boss::engines::RBL::Engine>();
  if(!engine && initialise) {
    engine.reset(new boss::engines::RBL::Engine());
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
