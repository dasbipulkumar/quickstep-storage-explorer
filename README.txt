Quickstep Storage Explorer 0.1
Copyright (c) 2011-2013 by the Quickstep Authors
See file CREDITS.txt for details.

This software is covered by the GPLv3 license. See COPYING.txt for details.

=== Introduction ===
The Quickstep Storage Explorer is an experimental platform for evaluating the
efficiency of various different storage organization choices in a main-memory
relational database. It is a complement to the paper "Design and Evaluation of
Storage Organizations for Read-Optimized Main Memory Databases" by Chasseur and
Patel, and can be used to replicate and build on the experiments in that paper.

The Storage Explorer contains a minimal subset of the Quickstep code base
necessary to conduct storage-oriented experiments. Additional releases with
more functionality will be forthcoming.

=== Building ===
See the file BUILDING.txt for specific instructions on how to build the
Quickstep Storage Explorer from source.

=== Using ===
The quickstep_storage_explorer binary takes a single argument which specifies
a JSON configuration file, like so:
./quickstep_storage_explorer config.json

quickstep_storage_explorer will generate the data specified by the
configuration using the specified physical organization, then run the series
of tests described and print their results.

The configuration file should contain a single JSON object, which describes all
experiment parameters. The following is a sample of such an object:
{
  "use_blocks":           true,
  "block_size_mb":        16,
  "table":                "narrow_u",
  "num_tuples":           250000000,
  "layout_type":          "columnstore",
  "sort_column":          0,
  "use_compression":      false,
  "index_column":         1,
  "num_runs":             10,
  "measure_cache_misses": true,
  "num_threads":          4,
  "thread_affinities":    [0, 2, 4, 6],
  "tests": [
    {
      "predicate_column":               0,
      "use_index":                      false,
      "sort_matches_before_projection": false,
      "selectivity":                    0.05,
      "projection_width":               3
    },
    {
      "predicate_column":               1,
      "use_index":                      true,
      "sort_matches_before_projection": false,
      "selectivity":                    0.05,
      "projection_width":               3
    },
    {
      "predicate_column":               2,
      "use_index":                      false,
      "sort_matches_before_projection": false,
      "selectivity":                    0.05,
      "projection_width":               3
    }
  ]
}

The following is a description of each attribute in a configuration object and
its effect:

*** "use_blocks": boolean
If true, Quickstep's native block-based organization will be used. If false,
file-based organization will be used (the number of equally-sized files will
be equal to "num_threads").

*** "block_size_mb": integer
The size, in megabytes, of blocks when "use_blocks" is true. Has no effect if
"use_blocks" is false.

*** "table": string, one of ["narrow_e", "narrow_u", "wide_e", "strings"]
Specifies which table schema to use for tests. Narrow-E has 10 32-bit integer
columns, with column i's values in the range [0 to 2^(2.7*(i+1))]. Narrow-U
has 10 32-bit integer columns, all with values in the range [0 to 100000000].
Wide-E has 50 32-bit integer columns, with column i's values in the range
[1 to 2^(4+(23/50)*i)]. Strings has 10 20-character string columns, all filled
with randomly generated characters.

*** "num_tuples": integer
The total number of tuples to generate in the specified "table".

*** "layout_type": string, one of ["rowstore", "columnstore"]
Whether to use a conventional unsorted row-store or a sorted column-store for
tuple storage.

*** "sort_column": integer
The ID of the column to sort on when using a column-store. Has no effect when
using a row-store.

*** "use_compression": boolean
If true, attempt to compress all columns in the test table using
dictionary-coding and bit-packing. Note that Narrow-U and Strings tables are
very unlikely to be successfully compressed, while lower-numbered columns
of Narrow-E and Wide-E are usually compressible.

*** "index_column": integer (optional)
If specified, build a CSBTree index on the column indicated. If "index_column"
is not specified, no index will be built.

*** "num_runs": integer
The number of distinct times to run each test before reporting the overall mean
and standard deviation of response times.

*** "measure_cache_misses": boolean
If true, use the Intel PCM library to precisely measure L2 and L3 cache misses
during query execution. Quickstep Storage Explorer must be build with Intel PCM
support, you must be running on a recent Intel CPU (Nehalem microarchitecture
or newer), and you must have access to the MSRs (model specific registers) on
your system (usually obtained by running as root). If you don't meet any of
these conditions, the program may fail to run, and you should try again with
"measure_cache_misses" set to false.

*** "num_threads": integer
The number of parallel execution threads to use for intra-query parallelism.
In the file-based organization, this will also be the number of equally-sized
files the table is partitioned into.

*** "thread_affinities": array of integers, "num_threads" in length (optional)
If specified, each individual execution thread will be pinned to the CPU core
ID specifed by its position in the array. This can be used to precisely control
which threads run on which logical core, which can be particularly useful when
trying to control for hyperthreading, multi-socket systems, etc. Note that this
may not be available on non-POSIX (i.e. Windows) systems.

*** "tests": array of objects
The "tests" array specifies a sequence of test queries to run on the specified
tables. Each query will be run "num_runs" times, and statistics on response
time and (optionally) cache misses will be reported.

Each object in the tests array specifies the following attributes:

**** "predicate_column": integer
The ID of the column to make a comparison predicate to select on.

**** "use_index": boolean
If true, an index will be used for predicate evaluation. If false, the
predicate will be evaluated directly on the base table. Note that
"predicate_column" must be the same as "index_column" in order to use an index.

**** "sort_matches_before_projection": boolean
If true, when using an index, a list of tuple-IDs matching the predicate will
built, then sorted into order before performing the projection on the base
table. This was found to sometimes improve performance for file-based column
stores. Has no effect if "use_index" is false.

**** "selectivity": float
The selectivity factor of the predicate, specified in the range (0.0, 1.0].

**** "projection_width": integer
The number of columns to project. Columns will be randomly chosen for each
experiment run. "projection_width" may be 0, in which case no projection is
performed (predicate evaluation will still be done, though).

