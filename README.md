# Data Lakehouse

### Short description

An AWS cloud-based solution for processing and transforming online transaction database into an online analytical database lakehouse using an ETL approach.

### Languages used

- Python
- Terraform
- SQL
- GitHub Workflow
- Amazon Web Services

### Overview

This group project was completed as part of the Northcoders Data Engineering course. It's goal was to create an application to extract, transform and load data from a transactional database to a STAR schema analytics database.

The minimum viable product produced consisted of:

- two S3 buckets for extracted and transformed data in parquet format
- three Python Lambdas to extact data from the OLTP database, transform data in to target STAR schemas and to load data into the OLAP database
- CloudWatch logs and alarms to monitor lambda performance

[![MVP diagram](https://github.com/runraga/nc-de-final-project/blob/main/diagrams/MVP.png?raw=true)](https://github.com/runraga/nc-de-final-project/blob/main/diagrams/MVP.png?raw=true)

A CI/CD automation pipeline was set up for the application using:

- Git Hooks to enforce fully unit tested (Jest), PEP8 compliant and 'safe' python code
- Terraform to manage infrastructure in AWS
- GitHub workflow for continuous deployment of code as it was commited to the main branch

### Future Features

- Improvements to integration testing
- Restructure ingestion bucket data to better track raw data
- Refactor to enable history tracking of transactions in OLAP database
