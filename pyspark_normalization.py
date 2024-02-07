from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL with PySpark") \
    .getOrCreate()

# Sample unnormalized data
unnormalized_data = [
    ('John Smith', 'Laptop', 'Electronics', 999.99, 2, '2024-02-07', 'Alice Johnson', 'john@example.com', '123 Main Street'),
    ('Emily Brown', 'Smartphone', 'Electronics', 599.99, 1, '2024-02-06', 'Bob Williams', 'emily@example.com', '456 Elm Avenue'),
    ('David Lee', 'Headphones', 'Electronics', 99.99, 3, '2024-02-05', 'Cindy Davis', 'david@example.com', '789 Oak Lane')
]

# Define schema for the unnormalized data
schema = [
    'CustomerName', 'ProductName', 'ProductCategory', 'ProductPrice', 'QuantitySold',
    'TransactionDate', 'SalesPerson', 'CustomerEmail', 'CustomerAddress'
]

# Create DataFrame from unnormalized data
df = spark.createDataFrame(unnormalized_data, schema)

# Define function to get or insert records into dimension tables and return the ID
def get_or_insert_dim_id(df_dim, unique_cols, table_name):
    # Check if record exists
    existing_record = df_dim.where(reduce(lambda x, y: x & y, (df_dim[col] == val for col, val in unique_cols.items())))
    if existing_record.count() > 0:
        return existing_record.select('ID').first()[0]  # Return existing ID
    else:
        # Insert new record
        new_record = spark.createDataFrame([unique_cols], list(unique_cols.keys()))
        new_record = new_record.withColumn("ID", monotonically_increasing_id())
        new_record.write.mode('append').format("parquet").saveAsTable(table_name)
        return new_record.select('ID').first()[0]  # Return newly generated ID

# Create dimension DataFrames
customer_dim = df.select('CustomerName', 'CustomerEmail', 'CustomerAddress').distinct()
customer_dim = customer_dim.withColumn("CustomerID", monotonically_increasing_id())

product_dim = df.select('ProductName', 'ProductCategory', 'ProductPrice').distinct()
product_dim = product_dim.withColumn("ProductID", monotonically_increasing_id())

salesperson_dim = df.select('SalesPerson').distinct()
salesperson_dim = salesperson_dim.withColumn("SalesPersonID", monotonically_increasing_id())

transaction_date_dim = df.select('TransactionDate').distinct()
transaction_date_dim = transaction_date_dim.withColumn("TransactionDateID", monotonically_increasing_id())

# Create fact DataFrame with foreign keys
fact_table = df.join(customer_dim, on='CustomerName') \
                .join(product_dim, on='ProductName') \
                .join(salesperson_dim, on='SalesPerson') \
                .join(transaction_date_dim, on='TransactionDate') \
                .select('CustomerID', 'ProductID', 'SalesPersonID', 'TransactionDateID', 'QuantitySold')

print("Normalized Fact Table:")
fact_table.show()

print("\nCustomer Dimension Table:")
customer_dim.show()

print("\nProduct Dimension Table:")
product_dim.show()

print("\nSalesperson Dimension Table:")
salesperson_dim.show()

print("\nTransaction Date Dimension Table:")
transaction_date_dim.show()

# Stop Spark session
spark.stop()
