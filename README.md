# AWS_ETL_Pipeline

Utilizes AWS Lambda, S3, Glue, Athena, and QuickSight to process, transform, and visualize data in a scalable and serverless environment. The main objective is to preprocess raw data, convert it into a star schema format, and store it in Amazon S3, where it can be queried and analyzed using Athena. Finally, Amazon QuickSight is used for data visualization and reporting.

<img width="569" alt="Architecture" src="https://github.com/NMurari7/AWS_ETL_Pipeline/assets/70143030/56e84fe7-b379-44a9-ab09-0f4ccb439167">

## Steps
Extract Data: The raw data is stored in S3 and can be in any format such as CSV, JSON, or Parquet.

Transform Data: The data is preprocessed and converted into a star schema. Pandas library is used for data processing. The Lambda function is configured to use Pandas layer to optimize performance.

Load Data: The processed data is stored back in S3 using Boto3 library. The data is saved in Parquet format for better query performance. The Glue Crawler is then used to catalog the data.

Query Data: Athena is used to query the data stored in S3. Athena is a serverless query service that can query data stored in S3 using SQL.

Visualize Data: QuickSight is used to create a dashboard for data visualization. QuickSight is a cloud-based business intelligence service that can create interactive visualizations and reports.

## Setup
To run the ETL pipeline, follow these steps:

Setup S3 Bucket: Create an S3 bucket to store the raw data and the processed data.

Setup Lambda Function: Create an AWS Lambda function with the required libraries such as Pandas, Boto3, and StringIO. The Lambda function should be configured. The layer should be added to function to use pandas.

Configure Glue Crawler: Create a Glue Crawler to catalog the processed data in S3.

Setup Athena: Configure Athena to query the data stored in S3.

Create QuickSight Dashboard: Create a QuickSight dashboard to visualize the data.

## Conclusion
This ETL pipeline provides a scalable and efficient solution for processing and analyzing large amounts of data stored in S3. The pipeline can be customized to handle different types of data and can be extended to include additional data sources. By using cloud-based services such as AWS Lambda, Glue Crawler, Athena, and QuickSight, the pipeline provides a cost-effective and scalable solution for data processing and analysis.
