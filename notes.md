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

- Keyspace: collection of tables
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
