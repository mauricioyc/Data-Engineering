# Introduction

> Get feedback from GitHub, CV, LinkedIn and cover letter.

## What is Data engineering

Taking raw data and cleaning it for whoever needs to use it

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

## 1.1 Relational Data Models

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

## 1.2 Non Relation Data Models

Less restricted and scalable approach.

- Large amounts of data
- Horizontal scalability
- High throughput (fast reads)
- Flexible schema
- High availability
- Store different data type formats
- Distributed users (low latency)

### 1.2.1 Apache Cassandra
- Keyspace: collection of tables
- Table: A group of partitions
- Rows: single items
- Partition: fundamental unit, collection of rows and how the data is distributed
- Primary key: partitioning key + clustering columns
- Columns: clustering and data, labeled element