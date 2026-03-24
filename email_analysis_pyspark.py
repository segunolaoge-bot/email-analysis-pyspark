from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

# Create Spark session
spark = SparkSession.builder.appName("Email Analysis").getOrCreate()

# Load the email dataset
emails_df = spark.read.csv(
    "/Volumes/teaching/datasets/assignment/emails.csv",
    header=True,
    inferSchema=True,
    multiLine=True
)

# Show the first few rows
print("Preview of dataset:")
emails_df.show(5, truncate=False)

# Select only the sender column and remove missing values
df_clean = emails_df.select("From").dropna()

# Convert sender email addresses to lowercase
df_clean = df_clean.withColumn("From", lower(col("From")))

# Count total number of emails
total_emails = df_clean.count()

# Count unique senders
unique_senders = df_clean.select("From").distinct().count()

# Calculate mean emails per sender
mean_per_sender = total_emails / unique_senders

# Print results
print(f"Total emails: {total_emails}")
print(f"Unique senders: {unique_senders}")
print(f"Mean emails per sender: {mean_per_sender:.2f}")

# Show top 10 most active senders
print("Top 10 most active senders:")
top_senders = (
    df_clean.groupBy("From")
    .count()
    .orderBy(col("count").desc())
)

top_senders.show(10, truncate=False)

# Stop Spark session
spark.stop()
