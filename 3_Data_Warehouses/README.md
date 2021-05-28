# Project: Data Modeling with Postgres
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Data Model
Two datasets are provided and loaded into two tables in our cluster:
- song dataset
- log dataset

Then a star schema was implemented with:
- songplays FACT table
- users, songs, artists and time DIMENTION tables

## Python scripts
- Run 'create_tables.py' first to create database and empty tables.
- Then run 'etl.py' to fill all tables accordingly.
- 'sql_queries.py' have all necessary queries. No need to run, just consult if necessary.
- Notebook 'test.ipynb' was used in development phase and debugging. Feel free to explore if necessary.
- File 'dwh.cfg' has some process configuration values.

## Notes
- Before you can run these scripts, you need to build the infrastructure on AWS to get some parameters into 'dwh.cfg' file