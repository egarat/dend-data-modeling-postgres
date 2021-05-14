# Project 1: Data Modeling with Postgres

## Introduction

Taken from project introduction:
> A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

> They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Purpose

The purpose of this project is to allow Sparkify to analyze the song play activities of their users. To achieve that, we have been provided with song and log datasets. These files need to be loaded to a fact and dimension tables of a star schema using an ETL process. The star schema allows Sparkify to run queries for their song activity analysis.

For this project, we have been provided with the information for the fact and dimension tables. Which have been defined as followed:

### Fact Table

**songplays**
<br>
Records in log associated with song plays.

Column | Data Type | Primary Key
--- | --- | :---:
songplay_id | serial | x
start_time | timestamp |
user_id | int
level | varchar
song_id | varchar
artis_id | varchar
session_id | int
location | varchar
user_agent | text

### Dimension Tables

**users**
<br>
Users in the app

Column | Data Type | Primary Key
--- | --- | :---:
user_id | int | x
first_name | varchar |
last_name | varchar |
gender | char |
level | varchar |

**songs**
<br>
Songs in the music database

Column | Data Type | Primary Key
--- | --- | :---:
song_id | varchar | x
title | varchar |
artist_id | varchar |
year | int |
duration | numeric |

**artists**
<br>
Artists in the music database

Column | Data Type | Primary Key
--- | --- | :---:
artist_id | varchar | x
name | varchar |
location | varchar |
latitude | numeric |
longitude | numeric |

**time**
<br>
Timestamps of records in songplays broken down into specific units

Column | Data Type | Primary Key
--- | --- | :---:
start_time | timestamp | x
hour | int |
day | int |
week | int |
month | int |
year | int |
weekday | int |

### Side Notes

While the provided star schema is sufficient for Sparkify, it could be further improved. For example we could extract the user agent information and perform analysis by browser or operating system used. Other than that, the schema does answer the most questions, such as where are the most active customers located or listen duration categorized by free and paid users etc.

## Project Structure

The project directory contains three types of files.

**Datasets**
<br>
- `data`: This directory holds the song and log datasets which will be the sources for the star schema tables.

**Juypter Notebooks**
<br>
- `etl.ipynb`: Describes the whole ETL process to load the data from the source files to the star schema tables. This notebook was used as an iterative process to define the ETL steps.
- `test.ipynb`: Queries to test the ETL process and see whether the data load was successful or not.

**Python Scripts**
<br>
- `sql_queries.py`: Holds all the DDL and DML which are required for ETL.
- `create_tables.py`: This script will create all the tables for the star schema. 
- `etl.py`: Is the power horse of the whole project. This script will read all the source data, transform them, and load to the tables.

## Usage

In order to run this project and be able to perform analysis, the scripts `create_tables.py` and `etl.py` must be executed using the Python interpreter. Both of this scripts have dependencies to the `sql_query.py` script.

Open up a terminal and change to the project root directory, then enter these two commands:

```bash
python create_tables.py
python etl.py
```

## Example Queries

### Query 1:

Find top 5 users with the most sessions

**Input**

```sql
SELECT songplays.user_id
      ,users.first_name
      ,users.last_name
      ,COUNT(DISTINCT songplays.session_id) AS Sessions
FROM songplays
    JOIN users
    ON songplays.user_id = users.user_id
GROUP BY songplays.user_id, users.first_name, users.last_name
ORDER BY COUNT(DISTINCT songplays.session_id) DESC
LIMIT 5;
```

**Output**

user_id | first_name | last_name | sessions
---: | ---: | ---: | ---:
26 | Ryan | Smith | 55
49 | Chloe | Cuevas | 42
80 | Tegan | Levine | 33
32 | Lily | Burns | 30
88 | Mohammad | Rodriguez | 24

### Query 2:

User count of all paid and free users

**Input**

```sql
SELECT level
      ,COUNT(DISTINCT user_id) AS total_users
FROM songplays
GROUP BY level;
```

level | total_users
---: | ---:
free | 82
paid | 22
