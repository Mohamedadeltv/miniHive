# miniHive

A SQL-to-MapReduce query compiler that translates SQL queries into distributed MapReduce jobs for execution on Hadoop.

## Overview

miniHive is a lightweight implementation of a query processing engine similar to Apache Hive. It takes SQL queries as input, converts them to relational algebra, optimizes them, and generates MapReduce jobs that can run on Hadoop HDFS.

## Features

- **SQL Parsing**: Translates SQL queries into relational algebra expressions
- **Query Optimization**: Applies optimization rules to reduce execution costs
- **MapReduce Generation**: Compiles relational algebra into MapReduce jobs
- **Local & HDFS Execution**: Run queries locally or on a Hadoop cluster

## Architecture

```
SQL Query
    │
    ▼
┌─────────────┐
│  sql2ra.py  │  ──▶  SQL to Relational Algebra
└─────────────┘
    │
    ▼
┌─────────────┐
│  raopt.py   │  ──▶  Query Optimization
└─────────────┘
    │
    ▼
┌─────────────┐
│  ra2mr.py   │  ──▶  Relational Algebra to MapReduce
└─────────────┘
    │
    ▼
MapReduce Jobs (Hadoop)
```

## Components

### 1. SQL Parser (`sql2ra.py`)
Parses SQL queries and converts them into relational algebra trees:
- Extracts SELECT columns → Projection
- Extracts FROM tables → Cross Product / Rename
- Extracts WHERE conditions → Selection

### 2. Query Optimizer (`raopt.py`)
Applies optimization rules to reduce query execution costs:
- **Selection Push-Down**: Pushes selection conditions closer to base tables
- **Selection Break-Up**: Splits AND conditions into separate selections
- **Join Introduction**: Converts cross products with conditions into joins
- **Projection Push-Down**: Eliminates unnecessary columns early in the query

### 3. MapReduce Compiler (`ra2mr.py`)
Generates MapReduce tasks for each relational algebra operator:
- `SelectTask`: Filters rows based on conditions
- `ProjectTask`: Removes unnecessary columns and eliminates duplicates
- `RenameTask`: Renames table/column references
- `JoinTask`: Performs distributed joins using map-side tagging

## Usage

### Basic Query Execution
```bash
python3 miniHive.py "SELECT DISTINCT name FROM CUSTOMER WHERE id = 42"
```

### With Optimization
```bash
python3 miniHive.py --O "SELECT DISTINCT name FROM CUSTOMER WHERE id = 42"
```

### Specify Scale Factor
```bash
python3 miniHive.py --O --SF 1 "SELECT DISTINCT * FROM CUSTOMER, ORDERS WHERE C_CUSTKEY = O_CUSTKEY"
```

### Local Execution
```bash
python3 miniHive.py --O --SF 1 --env LOCAL "SELECT DISTINCT N_NAME FROM NATION"
```

### HDFS Execution
```bash
python3 miniHive.py --O --SF 1 --env HDFS "SELECT DISTINCT N_NAME FROM NATION"
```

## Optimization Results

The projection push-down optimization achieves significant cost reductions:

| Query | Cost Reduction |
|-------|----------------|
| Q1    | 82.8%          |
| Q2    | 79.7%          |
| Q5    | 11.6%          |
| Q6    | 74.1%          |
| Q7    | 69.6%          |

## Data

Uses the TPC-H benchmark dataset which includes tables:
- CUSTOMER
- ORDERS
- LINEITEM
- NATION
- REGION
- SUPPLIER
- PART
- PARTSUPP

## Tech Stack

- **Python 3**
- **Apache Hadoop** - Distributed processing
- **HDFS** - Distributed file storage
- **Luigi** - Pipeline orchestration
- **radb** - Relational algebra parsing
- **sqlparse** - SQL parsing

## Project Structure

```
miniHive/
├── miniHive.py      # Main entry point
├── sql2ra.py        # SQL to Relational Algebra
├── raopt.py         # Query Optimizer
├── ra2mr.py         # Relational Algebra to MapReduce
├── costcounter.py   # Cost calculation
└── data/            # TPC-H benchmark data
```

## Author

Mohamed Abouhitta

## Acknowledgments

Built as part of the Scaling Database Systems course at University of Passau.