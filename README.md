# AWS_ETL_Pipeline

## Overview
The project uses an AWS Lambda function to preprocess and convert the raw data stored in S3 into a star schema. The Lambda function uses Pandas, Boto3, and StringIO libraries for data processing and S3 storage.

The processed data is then stored back in S3 using Glue Crawler catalog. This makes the data queryable using Athena, which can be used to create a QuickSight dashboard for data visualization.

## Steps
Extract Data: The raw data is stored in S3 and can be in any format such as CSV, JSON, or Parquet.

Transform Data: The data is preprocessed and converted into a star schema. Pandas library is used for data processing. The Lambda function is configured to use Pandas layer to optimize performance.

Load Data: The processed data is stored back in S3 using Boto3 library. The data is saved in Parquet format for better query performance. The Glue Crawler is then used to catalog the data.

Query Data: Athena is used to query the data stored in S3. Athena is a serverless query service that can query data stored in S3 using SQL.

Visualize Data: QuickSight is used to create a dashboard for data visualization. QuickSight is a cloud-based business intelligence service that can create interactive visualizations and reports.

## Setup
To run the ETL pipeline, follow these steps:

Setup S3 Bucket: Create an S3 bucket to store the raw data and the processed data.

Setup Lambda Function: Create an AWS Lambda function with the required libraries such as Pandas, Boto3, and StringIO. The Lambda function should be configured to trigger on an S3 event.

Configure Glue Crawler: Create a Glue Crawler to catalog the processed data in S3.

Setup Athena: Configure Athena to query the data stored in S3.

Create QuickSight Dashboard: Create a QuickSight dashboard to visualize the data.

Conclusion
This ETL pipeline provides a scalable and efficient solution for processing and analyzing large amounts of data stored in S3. The pipeline can be customized to handle different types of data and can be extended to include additional data sources. By using cloud-based services such as AWS Lambda, Glue Crawler, Athena, and QuickSight, the pipeline provides a cost-effective and scalable solution for data processing and analysis.
