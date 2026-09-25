# Distributed Skyline Queries on Apache Flink

Project for the **Special Topics in Databases** course at the Technical University of Crete (2025–26).

A streaming version of the three MapReduce skyline algorithms from *Chen, Hwang & Wu – "MapReduce Skyline Query Processing with a New Angular Partitioning Approach"*, built on the Apache Flink DataStream API with Apache Kafka.

## How it works

- **Input:** a generator streams tuples (10M+, with correlated, uniform or anti-correlated distributions) to a Kafka topic. A second generator sends queries to another topic at random intervals.
- **Processing:** tuples are partitioned across parallel Flink workers. Each worker keeps a local skyline using BNL, and the local skylines are merged into the global skyline.
- **Output:** when a query arrives, the current global skyline is written to an output Kafka topic, and performance metrics are printed.

## Algorithms

> **MR-Dim** - Partitions the data space along one dimension.

> **MR-Grid** - Partitions the data space into a grid of cells.

> **MR-Angle** - Partitions the data space by angle (hyperspherical coordinates), for better load balancing.

The three algorithms differ only in the Map (partitioning) step. The local skyline and merge steps are shared.

## Features

- Configurable **parallelism**, **algorithm**, **Kafka topics** and **number of dimensions** (2D and above)
- Experiments across data distributions and parallelism levels, measuring processing time, ingestion latency and throughput (see the report)

## Tech stack

Java 17 · Apache Flink 1.17 · Apache Kafka · Maven
