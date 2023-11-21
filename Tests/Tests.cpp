#define CATCH_CONFIG_RUNNER
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <array>
#include <catch2/catch.hpp>
#include <numeric>
#include <variant>
using std::string;
using std::vector;
using std::literals::string_literals::operator""s; // NOLINT(misc-unused-using-decls) clang-tidy bug
using boss::utilities::operator""_;                // NOLINT(misc-unused-using-decls) clang-tidy bug
using boss::Expression;
using boss::get;
using boss::expressions::CloneReason;
using Catch::Generators::random;
using Catch::Generators::take;
using Catch::Generators::values;

namespace {
std::vector<string> librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
boss::ComplexExpression getEnginesAsList() {
  return {"List"_, {}, boss::ExpressionArguments(librariesToTest.begin(), librariesToTest.end())};
};
} // namespace

// NOLINTBEGIN(readability-magic-numbers)
// NOLINTBEGIN(bugprone-exception-escape)
TEST_CASE("Basics", "[basics]") { // NOLINT
  REQUIRE(!librariesToTest.empty());
  auto eval = [](boss::Expression&& expression) mutable {
    return boss::evaluate("EvaluateInEngines"_(getEnginesAsList(), std::move(expression)));
  };

  SECTION("Relational (Ints)") {
    SECTION("Selection") {
      auto intTable = "Table"_("Value"_("List"_(2, 3, 1, 4, 1)));
      auto result = eval("Select"_(std::move(intTable), "Where"_("Greater"_("Value"_, 3))));
      CHECK(result == "Table"_("Value"_("List"_(4))));
    }

    SECTION("Join") {
      auto const dataSetSize = 10;
      std::vector<int64_t> vec1(dataSetSize);
      std::vector<int64_t> vec2(dataSetSize);
      std::iota(vec1.begin(), vec1.end(), 0);
      std::iota(vec2.begin(), vec2.end(), dataSetSize);

      auto adjacency1 = "Table"_("From"_("List"_(boss::Span<int64_t>(vector(vec1)))),
                                 "To"_("List"_(boss::Span<int64_t>(vector(vec2)))));
      auto adjacency2 = "Table"_("From2"_("List"_(boss::Span<int64_t>(vector(vec2)))),
                                 "To2"_("List"_(boss::Span<int64_t>(vector(vec1)))));

      auto result = eval("Join"_(std::move(adjacency1), std::move(adjacency2),
                                 "Where"_("Equal"_("To"_, "From2"_))));

      INFO(result);
      REQUIRE(get<boss::ComplexExpression>(result).getDynamicArguments().size() == 4);
      REQUIRE(!get<boss::ComplexExpression>(
                   get<boss::ComplexExpression>(result).getDynamicArguments()[0])
                   .getDynamicArguments()
                   .empty());
      CHECK(get<boss::ComplexExpression>(
                get<boss::ComplexExpression>(
                    get<boss::ComplexExpression>(result).getArguments().at(0))
                    .getArguments()
                    .at(0))
                .getArguments()
                .size() == dataSetSize);
      CHECK(get<boss::ComplexExpression>(
                get<boss::ComplexExpression>(
                    get<boss::ComplexExpression>(result).getArguments().at(1))
                    .getArguments()
                    .at(0))
                .getArguments()
                .size() == dataSetSize);
      CHECK(get<boss::ComplexExpression>(
                get<boss::ComplexExpression>(
                    get<boss::ComplexExpression>(result).getArguments().at(2))
                    .getArguments()
                    .at(0))
                .getArguments()
                .size() == dataSetSize);
      CHECK(get<boss::ComplexExpression>(
                get<boss::ComplexExpression>(
                    get<boss::ComplexExpression>(result).getArguments().at(3))
                    .getArguments()
                    .at(0))
                .getArguments()
                .size() == dataSetSize);
    }
  }

  SECTION("Relational (empty table)") {
    auto emptyCustomerTable =
        "Table"_("ID"_("List"_()), "FirstName"_("List"_()), "LastName"_("List"_()),
                 "BirthYear"_("List"_()), "Country"_("List"_()));
    auto emptySelect =
        eval("Select"_(emptyCustomerTable.clone(CloneReason::FOR_TESTING), "Greater"_("ID"_, 10)));
    CHECK(emptySelect == emptyCustomerTable);
  }
}

TEST_CASE("SimpleMultiWayJoin", "[join]") { // NOLINT
  REQUIRE(!librariesToTest.empty());
  auto eval = [](boss::Expression&& expression) mutable {
    return boss::evaluate("EvaluateInEngines"_(getEnginesAsList(), std::move(expression)));
  };

  SECTION("TwiceSamePairOfIndices") {
    auto join =
        "Join"_("Join"_("Table"_("A"_("List"_(1, 2)), "B"_("List"_(3, 4))),
                        "Table"_("C"_("List"_(3, 4, 6)), "D"_("List"_(4, 7, 10)),
                                 "E"_("List"_(32, 94, 77))),
                        "Where"_("Equal"_("B"_, "C"_))),
                "Table"_("F"_("List"_(4, 7)), "G"_("List"_(1, 2))), "Where"_("Equal"_("D"_, "F"_)));

    auto output = eval(std::move(join));
    CHECK((output == "Table"_("A"_("List"_(1, 2)), "B"_("List"_(3, 4)),
                                            "C"_("List"_(3, 4)), "D"_("List"_(4, 7)),
                                            "E"_("List"_(32, 94)), "F"_("List"_(4, 7)),
                                            "G"_("List"_(1, 2)))
	  ||
	  output == "Table"_("A"_("List"_(2, 1)), "B"_("List"_(4, 3)),
                                            "C"_("List"_(4, 3)), "D"_("List"_(7, 4)),
                                            "E"_("List"_(94, 32)), "F"_("List"_(7, 4)),
			                    "G"_("List"_(2, 1))))
	  );
  }

  SECTION("EmptyJoin") {
    auto join =
        "Join"_("Join"_("Table"_("A"_("List"_(1, 2)), "B"_("List"_(3, 4))),
                        "Table"_("C"_("List"_(3, 4, 6)), "D"_("List"_(4, 7, 10)),
                                 "E"_("List"_(32, 94, 77))),
                        "Where"_("Equal"_("B"_, "C"_))),
                "Table"_("F"_("List"_(1, 2)), "G"_("List"_(4, 7))), "Where"_("Equal"_("D"_, "F"_)));

    CHECK(eval(std::move(join)) == "Table"_("A"_("List"_()), "B"_("List"_()), "C"_("List"_()),
                                            "D"_("List"_()), "E"_("List"_()), "F"_("List"_()),
                                            "G"_("List"_())));
  }

  SECTION("DifferentPairOfIndices") {
    auto join =
        "Join"_("Join"_("Table"_("A"_("List"_(1, 2)), "B"_("List"_(3, 4))),
                        "Table"_("C"_("List"_(3, 4, 6)), "D"_("List"_(4, 7, 10)),
                                 "E"_("List"_(32, 94, 77))),
                        "Where"_("Equal"_("B"_, "C"_))),
                "Table"_("F"_("List"_(1, 2)), "G"_("List"_(4, 7))), "Where"_("Equal"_("D"_, "G"_)));

    auto output = eval(std::move(join));
    CHECK((output == "Table"_("A"_("List"_(1, 2)), "B"_("List"_(3, 4)),
                                            "C"_("List"_(3, 4)), "D"_("List"_(4, 7)),
                                            "E"_("List"_(32, 94)), "F"_("List"_(1, 2)),
                                            "G"_("List"_(4, 7)))
	  ||
	  output == "Table"_("A"_("List"_(2, 1)), "B"_("List"_(4, 3)),
                                            "C"_("List"_(4, 3)), "D"_("List"_(7, 4)),
                                            "E"_("List"_(94, 32)), "F"_("List"_(2, 1)),
			                    "G"_("List"_(7, 4))))
	  );
  }
}

TEST_CASE("OSM", "[OSM]") { // NOLINT
  REQUIRE(!librariesToTest.empty());
  auto eval = [](boss::Expression&& expression) mutable {
    return boss::evaluate("EvaluateInEngines"_(getEnginesAsList(), std::move(expression)));
  };

  auto OSMData1 =
      "Table"_("FirstBegin"_("List"_(1, 2, 3, 4, 5, 6, 4, 7, 1)),
               "FirstEnd"_("List"_(2, 3, 1, 5, 4, 5, 6, 3, 7)),
               "FirstLength"_("List"_(10.0, 7.0, 8.0, 2.0, 15.0, 12.0, 4.0, 20.0, 6.0)));

  auto OSMData2 =
      "Table"_("SecondBegin"_("List"_(1, 2, 3, 4, 5, 6, 4, 7, 1)),
               "SecondEnd"_("List"_(2, 3, 1, 5, 4, 5, 6, 3, 7)),
               "SecondLength"_("List"_(10.0, 7.0, 8.0, 2.0, 15.0, 12.0, 4.0, 20.0, 6.0)));

  auto OSMData3 =
      "Table"_("ThirdBegin"_("List"_(1, 2, 3, 4, 5, 6, 4, 7, 1)),
               "ThirdEnd"_("List"_(2, 3, 1, 5, 4, 5, 6, 3, 7)),
               "ThirdLength"_("List"_(10.0, 7.0, 8.0, 2.0, 15.0, 12.0, 4.0, 20.0, 6.0)));

  auto join1 =
      "Join"_(OSMData1.clone(CloneReason::FOR_TESTING), OSMData2.clone(CloneReason::FOR_TESTING),
              "Where"_("Equal"_("FirstEnd"_, "SecondBegin"_)));

  SECTION("First Join") {
    auto output = eval(join1.clone(CloneReason::FOR_TESTING));
    INFO(output);
    REQUIRE(!get<boss::ComplexExpression>(output).getDynamicArguments().empty());
    REQUIRE(
        !get<boss::ComplexExpression>(get<boss::ComplexExpression>(output).getDynamicArguments()[0])
             .getDynamicArguments()
             .empty());
    CHECK(get<boss::ComplexExpression>(
              get<boss::ComplexExpression>(
                  get<boss::ComplexExpression>(output).getDynamicArguments()[0])
                  .getDynamicArguments()[0])
              .getArguments()
              .size() == 11);
  }

  auto join2 = "Join"_(std::move(join1), OSMData3.clone(CloneReason::FOR_TESTING),
                       "Where"_("Equal"_("SecondEnd"_, "ThirdBegin"_)));

  SECTION("Second Join") {
    auto output = eval(join2.clone(CloneReason::FOR_TESTING));
    INFO(output);
    REQUIRE(!get<boss::ComplexExpression>(output).getDynamicArguments().empty());
    REQUIRE(
        !get<boss::ComplexExpression>(get<boss::ComplexExpression>(output).getDynamicArguments()[0])
             .getDynamicArguments()
             .empty());
    CHECK(get<boss::ComplexExpression>(
              get<boss::ComplexExpression>(
                  get<boss::ComplexExpression>(output).getDynamicArguments()[0])
                  .getDynamicArguments()[0])
              .getArguments()
              .size() == 15);
  }

  auto select1 = "Select"_(std::move(join2), "Where"_("Equal"_("ThirdEnd"_, "FirstBegin"_)));

  SECTION("First Select (to close the triangles)") {
    auto output = eval(select1.clone(CloneReason::FOR_TESTING));
    INFO(output);
    REQUIRE(!get<boss::ComplexExpression>(output).getDynamicArguments().empty());
    REQUIRE(
        !get<boss::ComplexExpression>(get<boss::ComplexExpression>(output).getDynamicArguments()[0])
             .getDynamicArguments()
             .empty());
    CHECK(get<boss::ComplexExpression>(
              get<boss::ComplexExpression>(
                  get<boss::ComplexExpression>(output).getDynamicArguments()[0])
                  .getDynamicArguments()[0])
              .getArguments()
              .size() == 9);
  }

  auto project =
      "Project"_(std::move(select1), "As"_("FirstLength"_, "FirstLength"_),
                 "As"_("SecondLength"_, "SecondLength"_), "As"_("ThirdLength"_, "ThirdLength"_),
                 "As"_("totalLength"_, "Plus"_("FirstLength"_, "SecondLength"_, "ThirdLength"_)));

  SECTION("Project with Arithmetic") {
    auto output = eval(project.clone(CloneReason::FOR_TESTING));
    INFO(output);
    REQUIRE(!get<boss::ComplexExpression>(output).getDynamicArguments().empty());
    REQUIRE(get<boss::ComplexExpression>(output).getDynamicArguments().size() == 4);
    REQUIRE(
        !get<boss::ComplexExpression>(get<boss::ComplexExpression>(output).getDynamicArguments()[0])
             .getDynamicArguments()
             .empty());
    CHECK(get<boss::ComplexExpression>(
              get<boss::ComplexExpression>(
                  get<boss::ComplexExpression>(output).getDynamicArguments()[0])
                  .getDynamicArguments()[0])
              .getArguments()
              .size() == 9);
  }

  auto select2 = "Select"_(std::move(project), "Where"_("Greater"_("totalLength"_, 30.0)));

  SECTION("Second Select (on total length)") {
    auto output = eval(select2.clone(CloneReason::FOR_TESTING));
    INFO(output);
    REQUIRE(!get<boss::ComplexExpression>(output).getDynamicArguments().empty());
    REQUIRE(
        !get<boss::ComplexExpression>(get<boss::ComplexExpression>(output).getDynamicArguments()[0])
             .getDynamicArguments()
             .empty());
    CHECK(get<boss::ComplexExpression>(
              get<boss::ComplexExpression>(
                  get<boss::ComplexExpression>(output).getDynamicArguments()[0])
                  .getDynamicArguments()[0])
              .getArguments()
              .size() == 6);
  }

  auto top10 = "Top"_(select2.clone(CloneReason::FOR_TESTING), 10,
                      "Multiply"_("FirstLength"_, "SecondLength"_, "ThirdLength"_));

  SECTION("Top 10") {
    auto output = eval(std::move(top10));
    INFO(output);
    REQUIRE(!get<boss::ComplexExpression>(output).getDynamicArguments().empty());
    REQUIRE(
        !get<boss::ComplexExpression>(get<boss::ComplexExpression>(output).getDynamicArguments()[0])
             .getDynamicArguments()
             .empty());
    CHECK(get<boss::ComplexExpression>(
              get<boss::ComplexExpression>(
                  get<boss::ComplexExpression>(output).getDynamicArguments()[0])
                  .getDynamicArguments()[0])
              .getArguments()
              .size() == 6);
  }

  auto top3 = "Top"_(select2.clone(CloneReason::FOR_TESTING), 3,
                     "Multiply"_("FirstLength"_, "SecondLength"_, "ThirdLength"_));

  SECTION("Top 3") {
    auto output = eval(std::move(top3));
    INFO(output);
    REQUIRE(!get<boss::ComplexExpression>(output).getDynamicArguments().empty());
    REQUIRE(
        !get<boss::ComplexExpression>(get<boss::ComplexExpression>(output).getDynamicArguments()[0])
             .getDynamicArguments()
             .empty());
    CHECK(get<boss::ComplexExpression>(
              get<boss::ComplexExpression>(
                  get<boss::ComplexExpression>(output).getDynamicArguments()[0])
                  .getDynamicArguments()[0])
              .getArguments()
              .size() == 3);
  }

  auto top1 = "Top"_(select2.clone(CloneReason::FOR_TESTING), 1,
                     "Multiply"_("FirstLength"_, "SecondLength"_, "ThirdLength"_));

  SECTION("Top 1") {
    auto output = eval(std::move(top1));
    CHECK(((output ==
          "Table"_("FirstLength"_("List"_(6.0)), "SecondLength"_("List"_(20.0)),
                   "ThirdLength"_("List"_(8.0)), "totalLength"_("List"_(34.0)))
	  ||
	  output ==
          "Table"_("FirstLength"_("List"_(8.0)), "SecondLength"_("List"_(6.0)),
                   "ThirdLength"_("List"_(20.0)), "totalLength"_("List"_(34.0))))
	  ||
	  output ==
          "Table"_("FirstLength"_("List"_(20.0)), "SecondLength"_("List"_(8.0)),
                   "ThirdLength"_("List"_(6.0)), "totalLength"_("List"_(34.0))))
	  );
  }
}

int main(int argc, char* argv[]) {
  Catch::Session session;
  session.cli(session.cli() | Catch::clara::Opt(librariesToTest, "library")["--library"]);
  auto const returnCode = session.applyCommandLine(argc, argv);
  if(returnCode != 0) {
    return returnCode;
  }
  return session.run();
}
// NOLINTEND(bugprone-exception-escape)
// NOLINTEND(readability-magic-numbers)
