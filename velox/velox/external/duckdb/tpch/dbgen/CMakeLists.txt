cmake_minimum_required(VERSION 3.0.0)
cmake_policy(SET CMP0063 NEW)

project(dbgen CXX C)

include_directories(include)

add_definitions(-DDBNAME=dss -DMAC -DORACLE -DTPCH)

add_library(
  dbgen STATIC
  dbgen.cpp
  dbgen_gunk.cpp
  bm_utils.cpp
  build.cpp
  permute.cpp
  rnd.cpp
  rng64.cpp
  speed_seed.cpp
  text.cpp)

# Suppress warnings when compiling dbgen.
target_compile_options(dbgen PRIVATE -w)
