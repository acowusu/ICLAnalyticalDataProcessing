#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <benchmark/benchmark.h>
#include <iostream>

namespace {

using namespace std;
using namespace boss;
using utilities::operator""_; // NOLINT(misc-unused-using-decls) clang-tidy bug

std::vector<string> librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
boss::ComplexExpression getEnginesAsList() {
  return {"List"_, {}, boss::ExpressionArguments(librariesToTest.begin(), librariesToTest.end())};
};

boss::Expression getTable(std::string fromAttribute, std::string toAttribute,
                          std::string lengthAttribute, size_t const dataSetSize) {
  return "Project"_("OSMData"_, "As"_(Symbol(fromAttribute), "beginID"_),
                    "As"_(Symbol(toAttribute), "endID"_),
                    "As"_(Symbol(lengthAttribute), "length"_));
}

boss::Expression addJoins(boss::Expression&& nestedJoin, size_t depth, size_t tableSize) {
  return ComplexExpression(Symbol("Join"),
                           ExpressionArguments( //

                               /* left input:*/ depth == 1
                                   ? std::move(nestedJoin)
                                   : addJoins(std::move(nestedJoin), depth - 1, tableSize),

                               /* right input: */
                               getTable("From" + to_string(depth), "To" + to_string(depth),
                                        "Length" + to_string(depth), tableSize),

                               /* condition: */
                               "Where"_("Equal"_(Symbol("To" + to_string(depth - 1)),
                                                 Symbol("From" + to_string(depth))))));
}

} // namespace

static void BenchmarkJoin(benchmark::State& state) {
  if(benchmark::internal::GetGlobalContext() != nullptr &&
     benchmark::internal::GetGlobalContext()->count("EnginePipeline")) {
    auto pipelines = benchmark::internal::GetGlobalContext()->at("EnginePipeline") + ";";
    while(pipelines.length() > 0) {
      librariesToTest.push_back(pipelines.substr(0, pipelines.find(";")));
      pipelines.erase(0, pipelines.find(";") + 1);
    }
  }

  auto const numberOfJoins = state.range(0);
  auto const tableSize = state.range(1);
  auto const minimalPathLength =
      (benchmark::internal::GetGlobalContext() != nullptr &&
       benchmark::internal::GetGlobalContext()->count("MinimalPathLength"))
          ? std::stoi(benchmark::internal::GetGlobalContext()->at("MinimalPathLength"))
          : numberOfJoins * 8.8;
  auto const topN = (benchmark::internal::GetGlobalContext() != nullptr &&
                     benchmark::internal::GetGlobalContext()->count("TopN"))
                        ? std::stoi(benchmark::internal::GetGlobalContext()->at("TopN"))
                        : numberOfJoins * 10;

  auto const prefix = (benchmark::internal::GetGlobalContext() != nullptr &&
                       benchmark::internal::GetGlobalContext()->count("URLPrefix"))
                          ? benchmark::internal::GetGlobalContext()->at("URLPrefix")
                          : "https://www.doc.ic.ac.uk/~dcl19/planet-";

  boss::evaluate("EvaluateInEngines"_(
      getEnginesAsList(), "Set"_("OSMData"_, "Load"_(tableSize, "beginID"_(prefix + "beginID.bin"),
                                                     "endID"_(prefix + "endID.bin"),
                                                     "length"_(prefix + "length.bin")))));

  ExpressionArguments lengthsForSelection, lengthsForTopN;
  for(auto i = 0u; i <= numberOfJoins; i++) {
    lengthsForSelection.push_back(Symbol("Length" + to_string(i)));
    lengthsForTopN.push_back(Symbol("Length" + to_string(i)));
  };

  auto const query = "EvaluateInEngines"_(
      getEnginesAsList(),
      "Top"_("Select"_(
                 "Select"_(addJoins(getTable("From0", "To0", "Length0", tableSize), state.range(0),
                                    tableSize),
                           "Where"_("Equal"_("From0"_, Symbol("To" + to_string(numberOfJoins))))),
                 "Where"_("Greater"_(ComplexExpression("Plus"_, std::move(lengthsForSelection)),
                                     minimalPathLength))),
             topN, ComplexExpression("Plus"_, std::move(lengthsForTopN))));


  // check if the query is valid
  auto result = boss::evaluate(query.clone(expressions::CloneReason::FOR_TESTING));
  if(get<boss::ComplexExpression>(result).getHead() == "ErrorWhenEvaluatingExpression"_) {
    std::stringstream output;
    output << result;
    state.SkipWithError(std::move(output).str());
    return;
  }

  for(auto _ : state) {
    state.PauseTiming();
    auto cloned = query.clone(expressions::CloneReason::FOR_TESTING);
    state.ResumeTiming();
    benchmark::DoNotOptimize(boss::evaluate(std::move(cloned)));
  }
}

BENCHMARK(BenchmarkJoin)
    ->Unit(benchmark::kMillisecond)
    ->ArgsProduct({benchmark::CreateDenseRange(1UL, 10UL, /*step=*/1),
                   benchmark::CreateRange(10UL, 2000UL * 1000 * 1000, /*multi=*/10)});
BENCHMARK_MAIN();
