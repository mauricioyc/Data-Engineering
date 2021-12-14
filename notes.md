# Introduction

This file contains my class notes from the Udacity Data Engineering Nanodegree.

## What is Data engineering

Taking raw data and cleaning it for whoever needs to use it.

## Data Hierarchy

Collect > Move/Store > **Explore/Transform > Aggregate/Label** > Learn/Optimize

Data Engineers stores and process data.

## Data Engineers Activities

- Ingest data from a data source
- Build and maintain a data warehouse
- Create a data pipeline
- Create an analytics table for a specific use case
- Migrate data to the cloud
- Schedule and automate pipelines
- Backfill data Debug data quality issues
- Optimize queries
- Design a database

---

# 1 Data Modeling

## **1.1 Relational Data Models**

More consistency and centered configuration.

- Atomicity: all or nothing is processed.
- Consistency: only constraint abided transactions allowed.
- Isolation: transactions are processed independently and securely.
- Durability: processed transactions are safely saved.

In relational database we want:

- Small data volumes
- Join tables (relations)
- Make aggregation
- ACID

### 1.1.1 Definitions

Database: organized and stored data.
RDBMS: software that manage the database.

- Standardize the data
- Flexibility in adding and altering tables
- Data integrity
- SQL
- Simplicity
- Intuitive organization

#### 1.1.2 OLAP (Online Analytical Processing) vs OLTP (Online Transactional Processing)

OLAP: Complex analytical and ad-hoc queries. optimized for reads.
OLTP: Less complex queries in large volumes. optimized for read, insert, update and delete.

#### 1.1.3 Normalization vs Denormalization

Normalization: reduce data redundancy and increase integrity.
Denormalization: read heavy workloads to increase performance.

##### 1.1.3.1 Normal Form

1NF: atomic, add data without changing everything, separate relations into different tables and foreign keys to relate tables.

2NF: all columns relies on the Primary Key

3NF: no transitive dependencies, avoid duplicated column data

##### 1.1.3.2 Denormalization

Adding redundancy to improve performance. Faster reads (select) and slow writes (insert, update, delete).

- Duplicate data
- Uses more space
- Harder to keep consistency

#### 1.1.4 Fact and Dimension Tables

Fact Table: measurements, metrics or facts of a business process.

Dimension table: structure facts to answer business questions. Dimensions are people, products, place and time.

##### 1.1.4.1 Star Schema

One or more fact tables related to many dimension tables.

Pro
- Denormalization
- Simplify queries
- Fast Aggregations
Cons
- Integrity
- Less query flexibility
- Many to many relationships

##### 1.1.4.2 Snowflake Schema

Dimensions is parent of multiple child tables, similarly to a star schema with more relations to the dimension tables.

### 1.1.5 Postgres

https://www.postgresqltutorial.com/postgresql-upsert/
https://www.postgresql.org/docs/9.5/sql-insert.html

## **1.2 Non Relation Data Models**

Less restricted and scalable approach.

- Large amounts of data
- Horizontal scalability
- High throughput (fast reads)
- Flexible schema
- High availability
- Store different data type formats
- Distributed users (low latency)

### 1.2.1 Distributed Database

Copied data in multiple machines to provide `high availability` and redundancy. It makes the data have `eventual consistency`, because it is complex to update redundant data.

### 1.2.2 CAP Theorem

Consistency: every read returns the latest and correct data.
Availability: every request is answered.
Partitioning Tolerance: the system continues to work even with node failure. 

### 1.2.3 Apache Cassandra
CAP -> AP system, it sacrifices consistency in critical situations.

- 9Keyspace: collection of tables
- Table: A group of partitions
- Rows: single items
- Partition: fundamental unit, collection of rows and how the data is distributed
- Primary key: partitioning key + clustering columns
- Columns: clustering and data, labeled element

#### 1.2.3.1 Data Modeling with Cassandra

`Query first`: **there are no joins or group by** in Apache Cassandra, thus `denormalization` is critical. You need to know a priori the queries you are going to perform in the data.

`One table per query`: creating a table for specific query is a good strategy. Usually HD space is cheap.

#### 1.2.3.2 Primary Key in Cassandra

Primary key is a unique key that can be comprised of a partition key and clustering key.
Tip: try to evenly distribute the data in the partition key.

#### 1.2.3.3 Clustering Columns

`Primary Key = Partitioning Key + (optional) Clustering Column`

The clustering column sorts the data in ascending order.

#### 1.2.3.4 Where Clause

A where clause must be included in Cassandra, unless a `ALLOW FILTERING` setting is provided, which is not recommended. A non primary key column cant be queried in a where clause.

# 2 Cloud Data Warehouses

## 2.1 Data Warehouse Definition

Separate operational process and analytical process. Operational databases are usually to slow for analytics, to hard to understand and requires many joins.

OLTP -> OLAP: The data warehouse is a system that enables analytical process, transactional data structured for data analysis.

Operational databases -> ETL -> Dimensional Database -> Business Analytics

- Simple to understand
- Analytical Performance
- Quality Assured
- Handles Ad-hoc queries
- Secure

## 2.2 Data Warehouse Architectures

### 2.2.1 Kimball's Bus Architecture

- Common dimension data model shared by different departments
- Atomic data
- Organized by business processes.

 &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;<-- Backroom --> <------- Front Room -------><br>
 &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;"Kitchen"&emsp;&emsp;&emsp;&emsp;&emsp;"Dining Room"<br>
Transactional Sources > ETL System &emsp;> &emsp;Presentation Area &emsp;&emsp;&emsp;> &emsp;&emsp;Applications <br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;(No user query) &emsp;(Atomic Conformed Dimensions)

Date dimension and product dimension is used across business process.

### 2.2.2 Independent Data Marts

- Independent ETL for each department
- Data Marts: separated smaller dimensional
- No conformity and inconsistent views

&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;<- Backroom -> <------ Front Room ------> <br>
Transactional Sources > ETL System > Department 1 < Applications <br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;> ETL System > Department 2 < Applications <br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;> ETL System > Department 3 < Applications <br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;> ETL System > Department n < Applications <br>

### 2.2.3 Corporate Information Factory (CIF)

- Independent Data Marts from organized DWH
- 2 ETLs: source to 3NF and 3NF to data marts
- Unlike Kimball's the data can be aggregated be cause there is already a 3NF DWH

<-------------------- Backroom --------------------> <br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;<------------------------ Front Room ------------------------> <br>
Data Acquisition > 3NF Database DWH > Data Delivery > Data Marts < Applications <br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;(Enterprise DWH)

### 2.2.4 Hybrid Kimball Bus and CIF

- Independent Data Marts from organized DWH
- 2 ETLs: source to 3NF and 3NF to data marts
- Unlike Kimball's the data can be aggregated be cause there is already a 3NF DWH

<-------------------- Backroom --------------------> <br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;<------------------------ Front Room ------------------------> <br>
Data Acquisition > 3NF Database DWH > Data Delivery > Data Marts < Applications
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;  3NF Database DWH < <<<<<<<<<<<<<<<<<< < Applications

## 2.3 OLAP Cubes

N Dimensional architecture, for example, Movie, Branch and Month. Easier to communicate information.

It should stores the finest gain of data (atom).

| Jan  | NY | Paris | SP |
|------|----|-------|----|
| Mov1 | 10 |  20   | 30 |
| Mov2 | 40 |  50   | 60 |

| Feb  | NY | Paris | SP |
|------|----|-------|----|
| Mov1 | 30 |  22   | 35 |
| Mov2 | 44 |  52   | 61 |

| Mar  | NY | Paris  | SP |
|------|----|--------|----|
| Mov1 | 60 |   26   | 32 |
| Mov2 | 46 |   58   | 66 |  

### 2.3.1 Operations:
- Roll-up: Aggregate columns in a dimension. For example, sum up the sales of cities by country

- Drill-down: Decompose columns in a dimension. For example, open the cities into districts.

- Slicing: Selecting a single dimension from the Cube from N to N-1 (from 3D make it 2D). For example, month="Mar"

- Dice: Selecting a sub-cube, For example, month=["Feb","Mar"]

### 2.3.2 OLAP Technologies

- MOLAP: pre-aggregate OLAP Cubes
- ROLAP: aggregates on the fly the OLAP Cubes
    -> columnar storage

## 2.4 Cloud Computing with AWS

### 2.4.1 Amazon Redshift

Redshift is a cluster with the main following properties:

- Postgres SQL Column oriented storage
- Massive Parallel Processing (MPP), parallelized query in CPUs and machines.
- Parallelized by partition

In Redshift, a `leader node` is the only node that communicates with the "outside world". It also coordinates the `computes nodes` and to optimize query execution.

The `leader node` tasks:
- Handle external communications.
- Coordinate compute nodes.
- Optimize queries.

The `compute node` tasks:
- Has its own CPU, memory and disk.
- Scale up configuration: few powerful nodes.
- Scale down configuration: lots of weaker nodes.
- Divided in slices. n slices equals to n table partition computation simultaneously.

### 2.4.2 ETL in AWS, Copying Data

In general, an EC2 instance issues a source to save files in a S3 and then issues the destination to read these files.

Source < EC2 > Destination

Source > S3 

S3 > Destination

In Redshift, it is highly recommended to use COPY statement in split compressed files. It is much faster than INSERT.

### 2.4.3 Optimizing Tables

#### 2.4.3.1 Even Distribution

- Evenly distribute the table, each CPU process is balanced.
- Not good for JOIN., lot of shuffle.

#### 2.4.3.2 All Distribution

"Broadcasting"

- Replicate the tables in each CPU. Small tables.
- Speed joins, no shuffles.

#### 2.4.3.3 Auto Distribution

- Leave the decision to Redshift.
- Small are EVEN.
- Large are ALL.
#### 2.4.3.4 Key Distribution
"distkey"
- Split the table into the CPU by key values.
- Skewed if key is not evenly distributed.
- It is efficient when the tables used to join are distributed on the same slices by the joining key.

#### 2.4.3.5 Sorting Key

- Sort key is defined in the loading of the table. The sorting key is loaded sorted.
- Speeds up highly requested columns which are usually sorted, such as data.

# 3 Data Lakes with Spark

## 3.1 Introduction to Big Data

Big numbers of processing and reading time.

- CPU: 0.4 ns
- RAM: 100 ns
- SSD Read: 16 micros
- Network: 150 ms

CPU 200x> Memory 15x> SSD 13x> HD speed

Network: the data in the clusters need to be shuffled. Tools such as Spark tries to minimize shuffling since network data transfer is often a bottleneck.

### 3.1.1 Hadoop Ecosystem

Consist in 4 main components:

- HDFS: distributed data storage.
- Map Reduce: large scale data processing.
- YARN: resource manager for the jobs across the cluster.
- Hadoop Common: utilities.

Example of tools:

- Pig: SQL like for map reduce.
- Hive: another SQL like for map reduce.
- Spark: fast memory computation for map reduce.
- Storm: streaming data.
- Flink: streaming data.

#### 3.1.2 Map Reduce

HDFS (split data in partitions) > 

MAP (transform the partitions in key value tuples) > 

Shuffle (organize the data by the keys to be in same machine) > 

Reduce (operation in the key value tuple, for example sum)

## 3.2 Spark

Spark is optimized to use memory efficiently and work on large datasets in a distributed system.

Modes:

- Standalone for testing and prototype.
- YARN cluster resource manager.
- Mesos cluster resource manager.

Use cases:

- ETL.
- Machine Learning.
- Data Streaming.
- Graph Analytics.

### 3.2.1 Functional Programming: Spark Scala

Functional programming. Sparks is made in Scala, different from procedural programming like Python, Scala is more strict in the function specifications and is perfect for distributed programming.

The main quality of the functional programming in a distributed system is its ability to preserve inputs and not set global variables, preserving the functions.

### 3.2.2 Spark Characteristics

- Lazy evaluation with DAGs.
- SQL or Python runs by a query optimizer called catalyst that are transformed into a DAG

Spark is RDD (Resilient Distributed Dataset):

- Memory
- Distributed
- Datasets

Spark uses accumulators to store global variables.

## 3.3 Data Lakes

Empowered by Big Data technologies. It gives higher flexibility to the data teams to explore closer to raw data.

A important step is to infer schema from from some standard data formats.

### 3.3.1 DW vs Data Lake

DW: high cost, strict format, high consistency, tabular data
Data Lake: low cost, lose format, exploratory and diverse data, scalable and parallel infrastructure

### 3.3.2 Data Lake Challenges

Garbage Dump: it is hard to define a minimum quality for the data in the lake.
Data Governance: it is hard to separate access by role in a data lake.
Dimensional Modeling: it is hard to define where the Lake and the DW should coexist. 

# 4 Data Pipelines

Data pipeline is a series of steps to process data. In general, it is used to create analytical data.

Process in a data pipeline:

- Data validation: ensure quality of the data, guarantee it is correct
- DAGs: it is very common to create data pipelines in directed acyclic graphs that are a special type of graph that does not have path that "cycles back" to a previous node.

## 4.1 Airflow

Airflow is a commonly used DAG based schedulable data pipeline tool that is easy to deploy in local or cloud environments.

Airflow is comprised by:

- Scheduler: orchestrate job executions.
- Queue: holds the stats of DAGs and Tasks.
- Worker: execute the tasks in the DAGs.
- Web Server: dashboard interface for visual control.

Instantiated Operators as considered Tasks.

### 4.1.1 Airflow Hooks

Hooks, allows users to manage connection and configuration of DAGs to external systems. There are many hooks out off the box in Airflow, such as `PostgresHook` for Redshift.

## 4.2 Data Quality

Data lineage is the steps of the data processing from the origin to the final destination. It is important to be able to show where the data comes from and how it is calculated to affirm data correctness and build trust with the data users.

### 4.2.1 Scheduled Data Processing

Schedules determine the period the data is analyzed. The naïve approach would be to analyze the whole data at all time. A better approach is to benefit from partitions and run schedules based on the size, periods and partitions of the data.

### 4.2.2 Data Partitioning in Data Pipelines

Partition often leads to more reliable data pipelines. Partitions can be any attribute and is usually based in dates.

Types of Partitioning:
- Logical: divide by conceptually related data.
- Size: limits data by the size (fill buckets of a given size).
- Location: separate by location is usually optimal for data analysis.
- Time: periods and dates usually are normally distributed.

Partition makes data have less dependency between each partition, making possible parallelizing the process.

### 4.2.3 Measure Data Quality

There must be requirements to meet data consumers quality. For example:

- Data size.
- Accuracy for a certain measure.
- Data source must be within a given timeframe.
- Check if a pipeline fails.
- No sensitive information.

## 4.3 Production Data Pipelines

- Reuse useful code. 
- Build custom Operators.
- Build custom hooks to access common APIs

In a data pipeline, it is a good practice to allow each task to be responsible for only one job it makes easier to abstract, understand, debug, parallelize and reuse the task.

### 4.3.1 Sub DAGs

Repeated tasks can be used in a sub DAG to:
- Shorten the code.
- Abstract DAG goal.
- Facilitate bug fixes.

On the other hand it:
- Makes the small steps in the pipeline less visible.
- Harder to understand.
- DAG architecture has to be thought from the start.

### 4.3.2 Monitoring

It is possible to integrate Airflow with Statsd and Grafana for easy task report. Also it is possible to set up email triggers and set SLAs to guarantee missed time schedule.