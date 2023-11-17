# Data Processing Systems -- Fall '23 Coursework (Version 1)

## Introduction

The objective of this coursework is to practice the implementation of join algorithms in a (semi-) real system. Specifically, you shall implement a multi-way join operator using hash-join and sort-merge-join primitives. You will implement it in a data management system my group is developing called ***BOSS***. BOSS is a research prototype of a next-generation data management system built on some ideas we developed in my research group. There are no systems like it out there, so let us start by providing you with some background knowledge:

## Background

### Challenges in Data Management System design

An increasingly relevant problem in data management is the diversity of available systems (to get an impression, browse the database of databases: [https://dbdb.io](https://dbdb.io)). Many of these systems are hyper-specialized but, still, built from scratch. BOSS follows a different approach: data management system composition.

BOSS is composed from **Engines**, functional units that have an extremely simple, unified interface:

```C++
class Engine { Expression evaluate(Expression&&); }
```

This unified interface allows the free composition of Engines into execution pipelines (any engine can process the output of any other engine).

As queries progress through the system, they are passed from engine to engine in the form of **Expressions**. An expression must, therefore, allow the representation of an incoming query plan, a result that is to be returned to the client and any stage in between. Naturally, this means that a query has to represent code (i.e., plans) and data in a single representation. This idea is known as homoiconicity.

### A Primer on Homoiconicity (with a focus on BOSS)

The idea of homoiconicity became popular with LISP, a class of languages in which everything (data and code) is a list. BOSS Expressions follow a similar pattern: they can be thought of as lists but the first element (the head) is a Symbol (a special string that encodes a variable or function name). The remaining elements of the list are called the arguments.

In BOSS, the head encodes the name of an operator, the first (and second in the case of a join) argument its input. The remaining arguments are parameters of the operator (potentially as expressions as well). A select would, e.g., be written (in lisp-syntax) like this:

```other
(Select input (Where (Greater someAttribute 17)))
```

a top-n operator (returning the top 10 according to `someAttribute`) would look like this

```other
(Top input someAttribute 10)
```

### A Primer on Partial Query Evaluation

Queries are evaluated in steps, with each engine in a pipeline making "some" progress towards the final result: data loading, query optimization, execution and potentially more. An engine, therefore, evaluates a query "partially".

A query may, for example come in as

```other
(GroupBy
(Select Lineitem
(Where Price > 17)) Count)
```

A storage engine would evaluate that (by replacing the Lineitem symbol with the content of the table) to

```other
(GroupBy (Select
(Table (Price 10 29 18 4 2)) (Where Price > 17))
Count)
```

An Evaluation engine would, finally, turn this into

```other
(Table (Count 2))
```

## Assignment

The composable architecture of BOSS allows developers to focus on a specific aspect of the system. In our case, the implementation of a very efficient multi-way join engine.

You shall implement an engine that processes a sequence of joins but leaves the rest of the plan unevaluated. The remainder of the operators will be processed by another engine that is implemented following the Volcano-model. Here is an illustration of the "big picture".

![Image.png](https://res.craft.do/user/full/0ab50382-64cb-9126-31a1-194104127cb6/doc/57F401AC-4F1B-4F38-B084-704F13C43C42/3C9B3F73-9555-43B5-AB9F-58637BC7A954_2/aGo1DExhdOhi9IBBJzN34sGjyrJdRTY0PqwDNgoSy98z/Image.png)

### Simplifications

While BOSS has engines supporting all of SQL (not included in the code you receive), we simplified things a bit.  The class of queries we consider in the coursework this allows the representation of arbitrary operators (even beyond relational algebra), we restrict ourselves to four operators:

   - Equi-Joins
   - Project
   - Select
   - Top-N

The class of queries we consider are cycle-counting queries on "graph-like" data. Each edge in the cycles we aim to find induces a join. A triangle-counting query would, e.g., induce a two-way join. Here is an example finding the 10 (or fewer) longest cycles with a length greater than 17:

```other
(Top 
 (Select 
  (Select 
   (Join
    (Join
     (Project 'OSMData (As FirstBegin beginID) (As FirstEnd endID) (As FirstLength length))
     (Project 'OSMData (As SecondBegin beginID) (As SecondEnd endID) (As SecondLength length))
     (Where (Equal FirstEnd SecondBegin)))
    (Project 'OSMData
             (As ThirdBegin beginID)
             (As ThirdEnd endID)
             (As ThirdLength length))
    (Where (Equal SecondEnd ThirdBegin)))
   (Where (Equal ThirdEnd FirstBegin))
   )
  (Where (Greater (Plus FirstLength SecondLength ThirdLength) 17))
  )
 10 (Plus FirstLength SecondLength ThirdLength)
 )
```

### Setup

#### Cloning

To get started, simply clone your Github repository: `git clone ${your repository url}`. Throughout the instructions, I will assume that your clone is located in `~/Projects/DPS-Coursework-2023`. If you clone it somewhere else, you, of course, have to adapt the instructions.

#### Building on a "properly configured" machine

```other
cd ~/Projects/DPS-Coursework-2023
mkdir build
cd build
cmake -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug ..
cmake --build .
```

#### Building on a lab machine

```Bash
cd ~/Projects/DPS-Coursework-2023
mkdir build
cd build
cmake -DCMAKE_CXX_COMPILER=clang++-14 -DCMAKE_BUILD_TYPE=Debug  -DCMAKE_CXX_FLAGS=-stdlib=libc++ ..
cmake --build .
```

## Your Task

We aimed to largely isolate you from the complexities of the system (plan parsing, handling partial query evaluation, etc.). To that end, we implemented a "simplification layer" that extracts chains of equi-joins from BOSS query plans and converts them into an easier-to-handle format, specifically, a vector of tables (which are represented as vectors of columns of values) and a vector of pairs of attributes. Each of the pairs connects two tables in the input. The i'th attribute pair specifies which attribute value in the i'th table has to be equal to which attribute in the (i+1)'th table to be considered a match. *Note that your implementation must handle non-unique input keys*.

You shall implement two engines, one implementing a sort-merge join, the other implementing a hash-join.

- Implement in two files
   - `JoinOnlyEngine/Source/SortMergeJoinOnly.cpp` 
   - `JoinOnlyEngine/Source/HashJoinOnly.cpp`

**Implement only in those files! You must not create any new files in the repository!**

**You must not use any templates beyond `vector`, `array`, `tuple`, `pair`, `function`, `visit` and `get`. No other containers, no algorithms. You are allowed to use any function from the c standard library.**

#### Testing

To test your implementation using some (very basic tests) you can run it in a pipeline with the volcano engine (which picks up the non-join operators). Just replace `libNestedLoopJoinOnlyEngine.so`, with your `libHashJoinOnlyEngine.so` or `libSortMergeJoinOnlyEngine.so`, in the following (and future mentions):

```Bash
./Tests --library JoinOnlyEngine/libNestedLoopJoinOnlyEngine.so\
        --library VolcanoEngine/libVolcanoEngine.so
```

the output should look like this

```other
===============================================================================
All tests passed (48 assertions in 3 test cases)
```

If you run the tests with only your join engine in the pipeline

```Bash
./Tests --library JoinOnlyEngine/libNestedLoopJoinOnlyEngine.so
```

many (though not all) of the tests will fail (unless you decide to implement the other operators as well):

```other
===============================================================================
test cases:  3 |  1 passed | 2 failed
assertions: 46 | 38 passed | 8 failed
```

### Benchmarking

- Once your implementation is functional and you are looking to optimize it, you should switch to a Release-build by running

```Bash
cmake -DCMAKE_BUILD_TYPE=Release .
```

Afterwards you should rebuild (`cmake --build .`) and run the benchmark (full pipeline)

```other
./Benchmarks --benchmark_context=EnginePipeline=\
$HOME/Projects/DPS-Coursework-2023/build/LoaderEngine/libLoaderEngine.so\;\
$HOME/Projects/DPS-Coursework-2023/build/JoinOnlyEngine/libNestedLoopJoinOnlyEngine.so\;\
$HOME/Projects/DPS-Coursework-2023/build/VolcanoEngine/libVolcanoEngine.so
```

You can benchmark parts of the pipeline by removing engines from the list.

## Submission

For your submission, you need to provide a brief (no more than 300 words) interpretation/explanation of the results in the file `Discussion.md`. Discuss questions like “How and why do your implementations scale?” and “Would a hybrid version potentially scale better?”

**You may implement and submit the coursework in teams of three.**

### The marks are distributed as follows

Note that passing tests are required but not necessarily sufficient for a correct implementation

- Correct & leak-free implementation of the sort-merge-join implementation: 35%
- Correct & leak-free implementation of the hash-join implementation: 35%
- Reasonable performance (no silly performance bugs, unnecessary function pointers, etc.): 10%
- Clean code with appropriate documentation: 10%
- Interpretation/explanation of the performance of your implementation: 10%
- You may earn some bonus marks by adding extra tests: 10%
- You may earn even more bonus marks by impressing us with a cool algorithm or optimization (talk to Holger if you want to assess an idea): 10%

## Competition — Sponsored by Snowflake

In addition to the implementation for marks, you can submit your implementation for the competition. You do so by adding a join implementation in `JoinOnlyEngine/Source/Competition.cpp` in your group's repository. For the competition, you are not bound to the rules regarding join algorithms -- anything goes. However, you may still only use the four listed templates. You may, further, not use static variables (i.e., propagate information from one run to the next) or any other tricks. We will manually inspect solutions that win and disqualify anyone who plays dirty.

#### Scoring

We will take three per-day scores in total: on November 13 (4 pm), November 20 (4 pm) and the day of submission. We will check your solution for correctness on a large dataset on the server side. We will run your solution on a dedicated lab-machine (spec-similar to, e.g., gpu18, gpu20 and gpu21)  using the open street maps dataset (i.e., using the same benchmark that comes with the framework). We will score according to the following rules:

- We define the number of joins in the plan as the "scale". We set the dataset size to 10^scale.
- We take the highest scale factor that at least three groups manage to run
- Within all solutions that manage to run that scale we take wallclock time and award (penalty) points, i.e., lower is better. They will be normalized to the fastest running solution
- Any team that does not make the cut (or does not submit) in a round will be scored a time that is 1.5 times that of the slowest solution that made the cut 
- In the end, we take a weighted sum of the three snapshots. The weights are 25/25/50 (last submission counts more)
- I will reward clean/readable/etc. code by deducting up to 10% from the final score based on my subjective assessment of code quality.

**The winner is the team with the lowest overall score**.

# Happy Coding!

