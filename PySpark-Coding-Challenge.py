# Databricks notebook source
# MAGIC %md
# MAGIC # Displaying Dataset 

# COMMAND ----------

# Display the table using PySpark DataFrame API
df = spark.table("loan")  # Assuming the table name is "loan"

# Show the first few rows of the table
df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Filter
# MAGIC

# COMMAND ----------

# Filter customers older than 40

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoanData").getOrCreate()

filtered_df = df.filter(df['Age'] > 40)

filtered_df.show()


# COMMAND ----------

# Filter married customers with Expenditure greater than 20,000
married_high_expenditure_sql_df = spark.sql("SELECT * FROM loan WHERE `Marital Status` = 'MARRIED' AND Expenditure > 20000")

married_high_expenditure_sql_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Joins

# COMMAND ----------

# Load the loan and credit tables as DataFrames
loan_df = spark.table("loan") 
credit_df = spark.table("credit")  

# Inner join between loan and credit tables on Customer_ID
inner_join_df = loan_df.join(credit_df, loan_df.Customer_ID == credit_df.CustomerId, 'inner')

inner_join_df.show()


# COMMAND ----------

# Left join between loan and credit tables on Customer_ID
left_join_df = loan_df.join(credit_df, loan_df.Customer_ID == credit_df.CustomerId, 'left')
left_join_df.show()


# COMMAND ----------

# Right join between loan and credit tables on Customer_ID
right_join_df = loan_df.join(credit_df, loan_df.Customer_ID == credit_df.CustomerId, 'right')
right_join_df.show()


# COMMAND ----------

# Outer join between loan and credit tables on Customer_ID
outer_join_df = loan_df.join(credit_df, loan_df.Customer_ID == credit_df.CustomerId, 'outer')
outer_join_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Simple aggregate functions

# COMMAND ----------

# Count the number of records in the loan table using PySpark
count_records = spark.sql("SELECT COUNT(*) AS total_records FROM loan")
count_records.show()

# Average Income Amount in the loan table using PySpark
average_income = spark.sql("""
    SELECT AVG(`Income`) AS avg_income 
    FROM loan
""")
average_income.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Group By

# COMMAND ----------

# Group by Marital Status and calculate the total Expenditure using PySpark
group_by_marital_status = spark.sql("""
    SELECT `Marital Status`, SUM(Expenditure) AS total_expenditure
    FROM loan
    GROUP BY `Marital Status`
""")
group_by_marital_status.show()

# Group by Loan Category and calculate the average Expenditure using PySpark
group_by_loan_category = spark.sql("""
    SELECT `Loan Category`, AVG(Expenditure) AS avg_expenditure
    FROM loan
    GROUP BY `Loan Category`
""")
group_by_loan_category.show()


