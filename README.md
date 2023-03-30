****Retraced Analytics Pipeline****


**Introduction**

The Retraced Corporation keeps track of user logins and the businesses that users control on its network. With this system, a person can be
a part of numerous companies. The business is interested in seeing how many of its companies are now engaged on their Retraced dashboard.

When a user accepts the terms and conditions, they are regarded as active for that specific company (T&C). In one company, a user may be active, 
but not yet in another. If there is at least one active user, the company considers the company to be active.

Retraced Business wants to publish this data on a frequent basis, so they need an analytics pipeline that can transform the database content into a 
format that is simple to query and is designed for quick querying.

You will receive raw CSV files from the business that contain randomized values for the user companies and user login locations columns. 
Create a pipeline that transforms these CSV files and saves the extracted data in a query-friendly manner.


**Requirements**

The following are the requirements for the project:

1. Build a pipeline with AWS services.
2. Store the raw CSV files in an S3 bucket.
3. Standardize the data using Python Lambda functions and convert the CSV input files to Parquet files to store in S3.
4. Use AWS Glue Crawler job to read Parquet files from S3 and store them in Athena tables.
5. Create a final serverless SQL query to show the following information:
    - The number of active users over time.
    - The number of active companies over time.
    - The potential active companies every month (all previously active companies, but which are not active any more in a month because no user has logged in).
6. Connect the Athena tables to Quicksight to visualize the data in a bar chart of total active companies over time.
7. Optimize the CSV files as Parquet files first and store them back in the data lake.


**Deliverables**

The following must be delivered as part of the project:

1. A pipeline built with AWS services that processes the raw CSV files and stores the resulting data in S3.
2. A final serverless SQL query that shows the required information.
3. Athena tables that store the data in a format optimized for querying.
4. A Quicksight dashboard that visualizes the data in a bar chart of total active companies over time.
5. Optimized CSV files as Parquet files stored in S3.


****Evaluation Criteria****

The following criteria will be used to evaluate your project:

- Understanding of the problem and requirements.
- Quality of the solution design.
- Quality of the code and data structures used.
- Performance of the pipeline and queries.
- Ability to communicate your thought process.



**Submission Guidelines**

Project must be submitted through a personal GitHub repository. The following should be in the repo:

- Code for the pipeline and Lambda functions.
- A SQL script with the final query.
- Documentation detailing how to run the pipeline and query the data.
- A README file detailing your thought process and explaining your solution design.



