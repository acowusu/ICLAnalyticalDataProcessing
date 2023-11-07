cmake_minimum_required(VERSION 3.10)
project(JoinOnlyEngine)

# Configure CCache if available
find_program(CCACHE_EXECUTABLE ccache)
mark_as_advanced(CCACHE_EXECUTABLE)
if(CCACHE_EXECUTABLE)
  foreach(LANG C CXX)
    if(NOT DEFINED CMAKE_${LANG}_COMPILER_LAUNCHER AND NOT CMAKE_${LANG}_COMPILER MATCHES ".*/ccache")
      message(STATUS "Enabling ccache for ${LANG}")
      set(CMAKE_${LANG}_COMPILER_LAUNCHER ${CCACHE_EXECUTABLE} CACHE STRING "")
    endif()
  endforeach()
endif()

############################## Custom build modes ###############################

set(CMAKE_CXX_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
  CACHE STRING
  "Flags used by the C++ compiler during Sanitize builds."
  FORCE)
set(CMAKE_C_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
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

############################### External Projects ###############################

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE) # make sure all dependent libraries are compiled with that option

if(MSVC)
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_LINK_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_LINK_LIBRARY_SUFFIX})
else()
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif(MSVC)

####################### Other Options and Library Dependencies #######################

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

#################################### Targets ####################################

if(MSVC)
  # not making any difference on Windows
  # and helps Visual Studio to parse it correctly as a target
  set(LibraryType SHARED)
else()
  set(LibraryType MODULE)
endif(MSVC)

set(NestedLoopJoinImplementationFiles Source/NestedLoopJoinOnly.cpp Source/Win32Boilerplate.cpp)

set(HashJoinImplementationFiles Source/HashJoinOnly.cpp Source/Win32Boilerplate.cpp)

set(SortMergeJoinImplementationFiles Source/SortMergeJoinOnly.cpp Source/Win32Boilerplate.cpp)

set(CompetitionImplementationFiles Source/Competition.cpp Source/Win32Boilerplate.cpp)

# NestedLoop
add_library(NestedLoopJoinOnlyEngine ${LibraryType} ${NestedLoopJoinImplementationFiles})
add_dependencies(NestedLoopJoinOnlyEngine BOSS)

#Hash
add_library(HashJoinOnlyEngine ${LibraryType} ${HashJoinImplementationFiles})
add_dependencies(HashJoinOnlyEngine BOSS)

#SortMerge
add_library(SortMergeJoinOnlyEngine ${LibraryType} ${SortMergeJoinImplementationFiles})
add_dependencies(SortMergeJoinOnlyEngine BOSS)

#Competition
add_library(CompetitionEngine ${LibraryType} ${CompetitionImplementationFiles})
add_dependencies(CompetitionEngine BOSS)

list(APPEND AllTargets NestedLoopJoinOnlyEngine HashJoinOnlyEngine SortMergeJoinOnlyEngine CompetitionEngine)

foreach(Target IN LISTS AllTargets)
  if(NOT WIN32)
    target_link_libraries(${Target} dl)
  endif(NOT WIN32)
    target_link_libraries(${Target} Threads::Threads)
    set_target_properties(${Target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    if(MSVC)
        target_compile_options(${Target} PUBLIC "/Zc:__cplusplus")
        target_compile_options(${Target} PUBLIC "/EHsc")
    endif()
    set_target_properties(${Target} PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
    add_dependencies(${Target} BOSS)
    target_include_directories(${Target} SYSTEM PUBLIC ${JoinOnlyEngine_BINARY_DIR}/deps/include)
    target_include_directories(${Target} SYSTEM PUBLIC ${CMAKE_BINARY_DIR}/deps/include)
    target_link_libraries(${Target} ${CMAKE_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX})
    if(UNIX AND NOT APPLE)
        target_link_libraries(${Target} rt)
    endif()
endforeach()

set_target_properties(NestedLoopJoinOnlyEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS NestedLoopJoinOnlyEngine LIBRARY DESTINATION lib)

set_target_properties(HashJoinOnlyEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS HashJoinOnlyEngine LIBRARY DESTINATION lib)

set_target_properties(SortMergeJoinOnlyEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS SortMergeJoinOnlyEngine LIBRARY DESTINATION lib)

set_target_properties(CompetitionEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS CompetitionEngine LIBRARY DESTINATION lib)
