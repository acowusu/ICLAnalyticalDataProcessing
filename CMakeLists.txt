cmake_minimum_required(VERSION 3.10)
project(DPS_BOSS)

############################## Custom build modes ###############################

if(NOT MSVC)
    set(DEFAULT_SANITIZER_COMPILER_FLAGS "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O2 -g")
else()
    set(DEFAULT_SANITIZER_COMPILER_FLAGS "/fsanitize=address /MD /Zi /O2 /Ob1 /DNDEBUG")
endif()

set(CMAKE_CXX_FLAGS_SANITIZE ${DEFAULT_SANITIZER_COMPILER_FLAGS}
  CACHE STRING
  "Flags used by the C++ compiler during Sanitize builds."
  FORCE)
set(CMAKE_C_FLAGS_SANITIZE ${DEFAULT_SANITIZER_COMPILER_FLAGS}
  CACHE STRING
  "Flags used by the C compiler during Sanitize builds."
  FORCE)
set(CMAKE_EXE_LINKER_FLAGS_SANITIZE
  ${CMAKE_EXE_LINKER_FLAGS_DEBUG} CACHE STRING
  "Flags used for linking binaries during Sanitize builds."
  FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  ${CMAKE_SHARED_LINKER_FLAGS_DEBUG} CACHE STRING
  "Flags used by the shared libraries linker during Sanitize builds."
  FORCE)
mark_as_advanced(
  CMAKE_CXX_FLAGS_SANITIZE		  CMAKE_EXE_LINKER_FLAGS_SANITIZE
  CMAKE_C_FLAGS_SANITIZE		  CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  )

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
  "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
  FORCE)

if(MSVC)
  # accurate profiling (closer to Release) in RelWithDebInfo: enable automatic inlining
  string(REPLACE "/Ob1" "/Ob2" CMAKE_C_FLAGS_RELWITHDEBINFO_INIT "${CMAKE_C_FLAGS_RELWITHDEBINFO_INIT}")
  string(REPLACE "/Ob1" "/Ob2" CMAKE_CXX_FLAGS_RELWITHDEBINFO_INIT "${CMAKE_CXX_FLAGS_RELWITHDEBINFO_INIT}")
  # better performances in Release
  string(REPLACE "/Ob2" "/Ob3" CMAKE_C_FLAGS_RELEASE_INIT "${CMAKE_C_FLAGS_RELEASE_INIT}")
  string(REPLACE "/Ob2" "/Ob3" CMAKE_CXX_FLAGS_RELEASE_INIT "${CMAKE_CXX_FLAGS_RELEASE_INIT}")
endif(MSVC)


############################### External Projects ###############################

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE) # make sure all dependent libraries are compiled with that option

if(MSVC)
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_LINK_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_LINK_LIBRARY_SUFFIX})
else()
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif(MSVC)

include(ExternalProject)

# Core BOSS
if(NOT DEFINED BOSS_SOURCE_BRANCH)
    set(BOSS_SOURCE_BRANCH main)
endif()
if(NOT DEFINED BOSS_SOURCE_REPOSITORY)
    set(BOSS_SOURCE_REPOSITORY https://gitlab.doc.ic.ac.uk/ham219/BOSS.git)
endif()
ExternalProject_Add(BOSS
    GIT_REPOSITORY ${BOSS_SOURCE_REPOSITORY}
    GIT_TAG ${BOSS_SOURCE_BRANCH}
    GIT_SHALLOW ON
    DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
		CMAKE_ARGS -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE} -DCMAKE_INSTALL_PREFIX=${DPS_BOSS_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
                   -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    BUILD_BYPRODUCTS ${DPS_BOSS_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX}
  )

if(WIN32)
  ExternalProject_Add_Step(BOSS copyDLL
      DEPENDEES install
      COMMAND ${CMAKE_COMMAND} -E copy ${DPS_BOSS_BINARY_DIR}/deps/bin/${CMAKE_SHARED_LIBRARY_PREFIX}BOSS${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${DPS_BOSS_BINARY_DIR}/
      BYPRODUCTS ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSS${CMAKE_SHARED_LIBRARY_SUFFIX})
endif(WIN32)
  
ExternalProject_Add(catch2
	URL https://github.com/catchorg/Catch2/archive/v2.13.6.tar.gz
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
	CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${DPS_BOSS_BINARY_DIR}/deps -DCATCH_BUILD_TESTING=NO -DBUILD_TESTING=NO -DCATCH_ENABLE_WERROR=NO -DCATCH_INSTALL_DOCS=NO
               -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
  )


################################# Engines #################################

add_subdirectory(VolcanoEngine)
include_directories(${VolcanoEngine_SOURCE_DIR}/Source)
link_directories(${VolcanoEngine_BINARY_DIR})

add_subdirectory(JoinOnlyEngine)
include_directories(${JoinOnlyEngine_SOURCE_DIR}/Source)
link_directories(${JoinOnlyEngine_BINARY_DIR})

add_subdirectory(LoaderEngine)
include_directories(${LoaderEngine_SOURCE_DIR}/Source)
link_directories(${LoaderEngine_BINARY_DIR})

if(WIN32)
   add_custom_command(DEPENDS VolcanoEngine
        COMMAND ${CMAKE_COMMAND} -E copy ${VolcanoEngine_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}VolcanoEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${DPS_BOSS_BINARY_DIR}/
      OUTPUT ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}VolcanoEngine${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_command(DEPENDS NestedLoopJoinOnlyEngine
        COMMAND ${CMAKE_COMMAND} -E copy ${JoinOnlyEngine_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}NestedLoopJoinOnlyEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${DPS_BOSS_BINARY_DIR}/
      OUTPUT ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}NestedLoopJoinOnlyEngine${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_command(DEPENDS HashJoinOnlyEngine
        COMMAND ${CMAKE_COMMAND} -E copy ${JoinOnlyEngine_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}HashJoinOnlyEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${DPS_BOSS_BINARY_DIR}/
      OUTPUT ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}HashJoinOnlyEngine${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_command(DEPENDS SortMergeJoinOnlyEngine
        COMMAND ${CMAKE_COMMAND} -E copy ${JoinOnlyEngine_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}SortMergeJoinOnlyEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${DPS_BOSS_BINARY_DIR}/
      OUTPUT ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}SortMergeJoinOnlyEngine${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_command(DEPENDS CompetitionEngine
        COMMAND ${CMAKE_COMMAND} -E copy ${JoinOnlyEngine_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}CompetitionEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${DPS_BOSS_BINARY_DIR}/
      OUTPUT ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}CompetitionEngine${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_command(DEPENDS LoaderEngine
        COMMAND ${CMAKE_COMMAND} -E copy ${LoaderEngine_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}LoaderEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${DPS_BOSS_BINARY_DIR}/
      OUTPUT ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}LoaderEngine${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_target(copyBossDLLs ALL
    DEPENDS ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}VolcanoEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
    DEPENDS ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}NestedLoopJoinOnlyEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
    DEPENDS ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}HashJoinOnlyEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
    DEPENDS ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}SortMergeJoinOnlyEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
    DEPENDS ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}CompetitionEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
    DEPENDS ${DPS_BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}LoaderEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
	)
endif(WIN32)

####################### Other Options and Library Dependencies #######################

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# required to support GCC < 9 and libstdc++ < 9 (see https://stackoverflow.com/a/33159746)
# still would compile for GCC >= 9 and libstdc++ >= 9 but with unnecessary experimental fs symbols
# we can get rid of it once we do not want to support anymore OS with old libstd (e.g. ubuntu bionic)
if(NOT WIN32)
  set(CXX_FILESYSTEM_LIBRARIES "stdc++fs")
endif()

include_directories(${DPS_BOSS_BINARY_DIR}/deps/include)
link_directories(${DPS_BOSS_BINARY_DIR}/deps/lib)

################################ Tests executable ################################

set(TestFiles Tests/Tests.cpp)

add_executable(Tests ${ImplementationFiles} ${TestFiles})
add_dependencies(Tests catch2)
add_dependencies(Tests BOSS)
target_link_libraries(Tests ${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX})
if(WIN32)
    target_compile_options(Tests PUBLIC /bigobj)
endif(WIN32)

target_link_libraries(Tests Threads::Threads)
set_target_properties(Tests PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
if(MSVC)
target_compile_options(Tests PUBLIC "/Zc:__cplusplus")
target_compile_options(Tests PUBLIC "/EHsc")
endif()
target_include_directories(Tests SYSTEM PUBLIC ${BOSS_BINARY_DIR}/deps/include)
if(UNIX AND NOT APPLE)
    target_link_libraries(Tests rt)
    target_link_libraries(Tests dl)
endif()

list(APPEND ExecutableTargets Tests)


############################# Benchmarks executable ############################

ExternalProject_Add(googlebenchmark
  URL "https://github.com/google/benchmark/archive/refs/tags/v1.8.3.tar.gz"
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${DPS_BOSS_BINARY_DIR}/deps -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON -DBENCHMARK_ENABLE_GTEST_TESTS=OFF $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}> -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
  BUILD_BYPRODUCTS ${DPS_BOSS_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX} ${DPS_BOSS_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark_main${CMAKE_STATIC_LIBRARY_SUFFIX}
  )
set_target_properties(googlebenchmark PROPERTIES EXCLUDE_FROM_ALL TRUE)


add_executable(Benchmarks Benchmarks/Benchmarks.cpp)
target_compile_options(Benchmarks PUBLIC -DBENCHMARK_STATIC_DEFINE)
set_target_properties(Benchmarks PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
target_link_libraries(Benchmarks Threads::Threads)
if(WIN32)
  target_link_libraries(Benchmarks shlwapi.lib)
endif(WIN32)
add_dependencies(Benchmarks googlebenchmark)
add_dependencies(Benchmarks BOSS)
target_link_libraries(Benchmarks ${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX})
target_link_libraries(Benchmarks ${DPS_BOSS_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX})

list(APPEND ExecutableTargets Benchmarks)


################################# Executable Common  #################################

foreach(Target IN LISTS ExecutableTargets)
    add_dependencies(${Target} VolcanoEngine) # dynamically loaded
    add_dependencies(${Target} NestedLoopJoinOnlyEngine) # dynamically loaded
    add_dependencies(${Target} HashJoinOnlyEngine) # dynamically loaded
    add_dependencies(${Target} SortMergeJoinOnlyEngine) # dynamically loaded
    add_dependencies(${Target} CompetitionEngine) # dynamically loaded
    add_dependencies(${Target} LoaderEngine) # dynamically loaded
endforeach()


#################################### Install ###################################

set_target_properties(Tests PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS Tests RUNTIME DESTINATION bin)
