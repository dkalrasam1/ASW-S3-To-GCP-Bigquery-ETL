@Author :  Deepak Sharma
@Created : 2022-04-23

# Data Extraction and load from AWS s3 to GCP Bigquery

Technology : Python, bigquery, aws s3, bigquery data transfer service

In this procedure, we retrieve data from an S3 storage, perform necessary transformations to meet our specifications, and subsequently upload the modified data to another S3 storage. Following this, we establish data transfer jobs in BigQuery to extract formatted data from the updated S3 storage and populate a BigQuery dataset.

It transfer many GB's of data everyday
