# Apache Airflow Adventures

This repo shows example projects of ETL Pipeline setups using Apache-Airflow. Most of the projects involve moving data from a data lake such as S3 to a datawarehouse for analysis.

## Projects

### Airflow: S3-to-Redshift

A dag that relies on AWS, s3 and Redshift hooks to copy json data from an S3 bucket into Redshift

### Airflow: S3-to-Redshift-Plugin

A parameterizable and reusable S3 to Redshift Plugin that can be used in any dag(without code copy)
**To be contributed soon**
