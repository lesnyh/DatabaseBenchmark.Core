# DatabaseBenchmark.Core

## Overview
**DatabaseBenchmarking.Core** is a general library for database performance evaluation. It provides robust tools for testing the performance of different databases (SQl, NoSQL (key-value store, object and etc) ),  generate reports (in CSV and JSON format) and a universal programming interface for implementing new databases into the library.

The benchmark engine performs two main test scenarios:

* Insertion of large amount of randomly generated records with sequential or random keys.
* Read of the inserted records, ordered by their keys.

During the tests, the following parameters are measured:

* Insert speed – the speed of insertion of all generated records (with sequential or random keys).
* Read speed – the speed of reading of all inserted records ordered by their key.
* Size – the size of the database after insert and read complete.

Every tested database must be capable of performing this simple test - insert the generated records and read them, ordered by their keys.

## Use cases
DatabaseBenchmark.Core can be used by developers to build simple or advanced database performance evaluation tools and applications.

The library is multi-platform and can also be compiled under Microsoft .NET Framework 4.0+ or Mono 4.0.0+ for use in Windows or Linux distributions.

Applications that are based on DatabaseBenchmark.Core are:

* **Database Benchmark** - one of the most powerfull open source tools designed to stress test databases: https://github.com/STSSoft/DatabaseBenchmark 
* **Database Benchmark for Console** - a multi-platform console implementation of Database Benchmark: https://github.com/STSSoft/DatabaseBenchmark.Console
