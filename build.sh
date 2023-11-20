#cd /vol/bitbucket/ao921/ICLAnalyticalDataProcessing
#mkdir build
cd build
cmake -DCMAKE_CXX_COMPILER=clang++-14 -DCMAKE_BUILD_TYPE=Debug  -DCMAKE_CXX_FLAGS=-stdlib=libc++ ..
cmake --build .