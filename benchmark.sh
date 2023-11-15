cmake -DCMAKE_CXX_COMPILER=clang++-14 -DCMAKE_BUILD_TYPE=Release   -DCMAKE_CXX_FLAGS=-stdlib=libc++ ..
cmake --build .
./Benchmarks --benchmark_context=EnginePipeline=\
$HOME/Projects/DPS-Coursework-2023/build/LoaderEngine/libLoaderEngine.so\;\
$HOME/Projects/DPS-Coursework-2023/build/JoinOnlyEngine/libNestedLoopJoinOnlyEngine.so\;\
$HOME/Projects/DPS-Coursework-2023/build/VolcanoEngine/libVolcanoEngine.so