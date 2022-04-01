# Sparkify Airflow Data Pipeline

This project builds a data pipeline for Sparkify and uses Apache Airflow to automate the running and monitoring of the pipeline.

The ETL process loads user log and song data in JSON format from S3 into analytics tables in a star schema on Redshift. A star schema has been implemented to allow the Sparkify team to readily run queries to analyze user activity on their app. Airflow regularly schedules this ETL and monitors its success by running a data quality check.

## File Structure

* `udac_example_dag.py` builds the dag dependencies using the various tasks defined by operators. Make sure this is in the `dags` directory of your Airflow installation.
* `create_tables.sql` contains all of the SQL queries used to create the necessary tables in Redshift. Make sure this is in the `dags` directory of your Airflow installation.
* `sql_queries.py` contains the SQL queries used in the ETL process. Make sure this is in the `plugins/helpers` directory of your Airflow installation.

The following operators should be placed in the `plugins/operators` directory of
your Airflow installation:
* `stage_redshift.py` contains `StageToRedshiftOperator`, which copies JSON data from S3 to staging tables in the Redshift data warehouse.
* `load_dimension.py` contains `LoadDimensionOperator`, which loads a dimension table from data in the staging table(s).
* `load_fact.py` contains `LoadFactOperator`, which loads a fact table from data in the staging table(s).
* `data_quality.py` contains `DataQualityOperator`, which runs a data quality check by passing an SQL query and expected result as arguments, failing if the results don't match.

## Configuration

* Make sure to add the following Airflow connections:
    * Amazon web services connection called "aws_credentials". This will contain your access key ID and secret access key (in the login and password fields).
    * Postgres connection called "redshift". This will contain your cluster endpoint, db name, login credentials, and port number (5439).