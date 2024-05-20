from pyspark.sql import SparkSession
import sys

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TableRowCountChecker") \
    .enableHiveSupport() \
    .getOrCreate()

# List of tables to check (include database name if necessary)
tables = ["database1.table1", "database2.table2", "database3.table3"]

# Function to check row count for specified tables and exit with error if count is zero
def check_table_counts(tables):
    zero_count_tables = []
    
    for table in tables:
        try:
            df = spark.table(table)
            count = df.count()
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
check_table_counts(tables)

# Stop the Spark session
spark.stop()
