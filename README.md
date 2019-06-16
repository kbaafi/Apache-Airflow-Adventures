# Apache Airflow Adventures

This repo shows example projects of ETL Pipeline setups using Apache-Airflow. Most of the projects involve moving data from a data lake such as S3 to a datawarehouse for analysis.

## Projects

### Airflow: S3-to-Redshift

A dag that relies on AWS, s3 and Redshift hooks to copy json data from an S3 bucket into Redshift

### Airflow: S3-to-Redshift-Plugin

A parameterizable and reusable S3 to Redshift Plugin that can be used in any dag(without code copy)

Uses 2 user created plugins. One for S3 to Redshift data transfer and another for verifying that data copied into Redshift

### Airflow: Subdag

The main dag does the following:

* Initialize tables in redshift
* Copy data from S3 to redshift using the subdag
* Load other tables by querying the staged data

The subdag that performs three functions:

* Optionally create a staging table in Redshift
* Copy staging data from Redshift to S3
* Verify if staging data was copied

> The subdag uses the 2 plugins created in the previous exercise. The demonstrates how you could build very ETL solutions using Redshift

A failure in any of these tasks should cause the whole subdag to fail.
