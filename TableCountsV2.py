from pyspark.sql import SparkSession
import sys
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TableRowCountChecker") \
    .enableHiveSupport() \
    .getOrCreate()

# Dictionary of tables with their corresponding partition columns
# Example: {"database1.table1": "partition_column1", "database2.table2": "partition_column2"}
tables_with_partitions = {
    "database1.table1": "partition_column1",
    "database2.table2": "partition_column2",
    "database3.table3": "partition_column3"
}

# Calculate the value date (for example, today)
value_date = datetime.now().strftime('%Y-%m-%d')

# Function to check row count for specified tables and exit with error if count is zero
def check_table_counts(tables_with_partitions, value_date):
    zero_count_tables = []
    
    for table, partition_col in tables_with_partitions.items():
        try:
            df = spark.table(table)
            count = df.filter(df[partition_col] == value_date).count()
            if count == 0:
                zero_count_tables.append(table)
        except Exception as e:
            print(f"Error accessing table {table}: {e}")
            sys.exit(1)  # Exit with error code 1 for table access error

    # If there are any tables with zero count, print message and exit with error
    if zero_count_tables:
        for table in zero_count_tables:
            print(f"Count is zero for table: {table}")
        sys.exit(2)  # Exit with error code 2 for zero row count

# Run the function
check_table_counts(tables_with_partitions, value_date)

# Stop the Spark session
spark.stop()
