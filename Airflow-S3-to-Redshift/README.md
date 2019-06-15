# Airflow: Loading data from S3 to Redshift

>This code vignette teaches you how to load data from S3 to redshift .The example stages some data on songs in Redshift and loads said data into **Songs** and **Artists** tables. Quality checks and data partitioning is not applied to the input data.

## Running the dag

* Create the folder **S3-to-Redshift** in your dags folder
* Copy the file *S3-to-Redshift.py* to your newly created folder. Make sure to edit line 6 in *S3-to-Redshift.py* to reflect your file system
* Copy *sql.py* to the newly created folder

Now you should be ready to run your dag in the airflow web ui

## Loading data

The original data can be retrived from **s3://udacity-dend/song_data**

Check the **stage_songs** in **sql.py** for the copy command that loads the data from S3 to Redshift

The airflow dag is defined in **S3-to-Redshift.py**
