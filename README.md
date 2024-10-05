# Airflow Homework

### Apache Airflow DAG was implemented which orchestrates tasks:
- **Task 1:** Connecting to molecule db and load every molecule into Pandas dataframe, then temporarily save it as CSV file;
- **Task 2:** Load temporary CSV file into dataframe and modify it. Modifications includes adding columns for extra molecular properties and the Lipinski pass. At last dataframe is saved as an Excel file;
- **Task 3:** Upload Excel file to AWS S3 bucket

### Dag is shceduled so that it will be executed daily.