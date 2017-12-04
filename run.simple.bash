#!/bin/bash

# simple script to run against running MySQL/MariaDB/Percona server localhost:(default port)

# database in which to run the benchmark
#   valid values : character
export MYSQL_DATABASE=test

# total number of simultaneous insertion threads
#   valid values : integer > 0
export NUM_LOADER_THREADS=4

# run the benchmark for this many inserts (or the number of minutes defined by RUN_MINUTES)
#   valid values : integer > 0
export MAX_ROWS=1000000000

# total number of rows to insert per "batch"
#   valid values : integer > 0
export NUM_ROWS_PER_INSERT=250

# display performance information every time the client application inserts this many rows
#   valid values : integer > 0, set to -1 if using NUM_SECONDS_PER_FEEDBACK
export NUM_INSERTS_PER_FEEDBACK=-1

# display performance information every time the client application has run for this many seconds
#   valid values : integer > 0, set to -1 if using NUM_INSERTS_PER_FEEDBACK
export NUM_SECONDS_PER_FEEDBACK=10

# run the benchmark for this many minutes (or the number of inserts defined by MAX_ROWS)
#   valid values : intever > 0
export RUN_MINUTES=60
export RUN_SECONDS=$[RUN_MINUTES*60]

# total number of rows to insert per second, allows for the benchmark to be rate limited
#   valid values : integer > 0
export MAX_INSERTS_PER_SECOND=999999

# number of additional character fields (semi-compressible) to add to each inserted row
#   valid values : integer >= 0
export NUM_CHAR_FIELDS=0

# size (in bytes) of each additional semi-compressible character field
#   valid values : integer >= 0
export LENGTH_CHAR_FIELDS=1000

# percentage of highly compressible data (repeated character "a") in character field
#   valid values : integer >= 0 and <= 100
export PERCENT_COMPRESSIBLE=90

# number of secondary indexes to maintain
#   valid values : integer >= 0 and <= 3
export NUM_SECONDARY_INDEXES=3

# the following 4 parameters allow an insert plus query workload benchmark

# number of queries to perform per QUERY_INTERVAL_SECONDS seconds
#   valid values : integer > 0, set to zero for insert only workload
export QUERIES_PER_INTERVAL=0

# number of seconds during which to perform QUERIES_PER_INTERVAL queries
#   valid values : integer > 0
export QUERY_INTERVAL_SECONDS=15

# number of rows to return per query
#   valid values : integer > 0
export QUERY_LIMIT=10

# wait this many inserts to begin the query workload
#   valid values : integer > 0
export QUERY_NUM_ROWS_BEGIN=1000000

# if InnoDB, compress to 1K, 2K, 4K, 8K, 16K block. Pass 0 for no InnoDB compression.
#   valid values : integer in (0,1,2,4,8,16)
export INNODB_KEY_BLOCK_SIZE=0

# Storage engine for benchmark
#   valid values : tokudb, innodb, myisam
export MYSQL_STORAGE_ENGINE=tokudb

# MySQL server port
#   valid values : integer > 0
export MYSQL_PORT=27017

# name of the server to connect to
export MYSQL_SERVER=localhost

# mysql username
export MYSQL_USERNAME=tmc

# mysql password
export MYSQL_PASSWORD=tmc

# create the table (Y/N)
export CREATE_TABLE=Y



# if TokuDB, need to select compression for primary key and secondary indexes (zlib is default)
#   valid values : lzma, quicklz, zlib, none
#export TOKUDB_COMPRESSION=zlib

# if TokuDB, need to select basement node size (65536 is default)
#   valid values : integer > 0 : 65536 for 64K
#export TOKUDB_BASEMENT=65536



javac -cp $CLASSPATH:$PWD/src src/jiibench.java


export LOG_NAME=iibench-${MAX_ROWS}-${NUM_ROWS_PER_INSERT}-${MAX_INSERTS_PER_SECOND}-${NUM_LOADER_THREADS}-${QUERIES_PER_INTERVAL}-${QUERY_INTERVAL_SECONDS}.txt
export BENCHMARK_TSV=${LOG_NAME}.tsv

rm -f $LOG_NAME
rm -f $BENCHMARK_TSV

T="$(date +%s)"
java -cp $CLASSPATH:$PWD/src jiibench $MYSQL_DATABASE $NUM_LOADER_THREADS $MAX_ROWS $NUM_ROWS_PER_INSERT $NUM_INSERTS_PER_FEEDBACK \
                                      $NUM_SECONDS_PER_FEEDBACK $BENCHMARK_TSV $RUN_SECONDS $QUERIES_PER_INTERVAL $QUERY_INTERVAL_SECONDS \
                                      $QUERY_LIMIT $QUERY_NUM_ROWS_BEGIN $MAX_INSERTS_PER_SECOND $NUM_CHAR_FIELDS $LENGTH_CHAR_FIELDS \
                                      $NUM_SECONDARY_INDEXES $PERCENT_COMPRESSIBLE $MYSQL_PORT $MYSQL_STORAGE_ENGINE $INNODB_KEY_BLOCK_SIZE \
                                      $MYSQL_SERVER $MYSQL_USERNAME "$MYSQL_PASSWORD" $CREATE_TABLE | tee -a $LOG_NAME

echo "" | tee -a $LOG_NAME
T="$(($(date +%s)-T))"
printf "`date` | iibench duration = %02d:%02d:%02d:%02d\n" "$((T/86400))" "$((T/3600%24))" "$((T/60%60))" "$((T%60))" | tee -a $LOG_NAME
