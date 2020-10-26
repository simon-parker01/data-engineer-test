# data-engineer-test

This project takes weather data in a CSV format and transforms it into parquet suitable for querying.

# Folders

* ## Pandas
  *  This folder contains a jupyter notebook used to investigate the data provided
  *It also contains the data in CSV and parquet format

* ##  Glue
  * This folder contains an AWS glue script that performs an ETL. Taking the data from  CSV files in one s3 bucket and writing it to another as  a parquet file
  ### Athena
   * Contains an athena query to answer the question "When and where was the hotteset day"
 
**Note** This ETL makes use of two AWS Glue crawlers to populate a data cataglue. One to catalogue the CSV data in the s3 bucket fur use by the glue script and one to catalogue the parquet data for use in Athena

