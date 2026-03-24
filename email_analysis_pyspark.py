from pyspark.sql import functions as F

# Load the dataset
df_raw = spark.read.csv(
    "/Volumes/teaching/datasets/assignment/emails.csv",
    header=True,
    inferSchema=True,
    quote='"',
    escape='"',
    multiLine=True
)

# Extract useful fields from raw email text
df_parsed = (
    df_raw
    .withColumn("From", F.regexp_extract("message", r"(?im)^From:\s*(.*)$", 1))
    .withColumn("To", F.regexp_extract("message", r"(?im)^To:\s*(.*)$", 1))
    .withColumn("Date", F.regexp_extract("message", r"(?im)^Date:\s*(.*)$", 1))
    .withColumn("Subject", F.regexp_extract("message", r"(?im)^Subject:\s*(.*)$", 1))
)

# Clean extracted fields
df_clean = (
    df_parsed
    .filter((F.col("From").isNotNull()) & (F.col("From") != ""))
    .filter((F.col("Date").isNotNull()) & (F.col("Date") != ""))
    .withColumn("From", F.lower(F.trim(F.col("From"))))
    .withColumn("To", F.lower(F.trim(F.col("To"))))
    .withColumn("Subject", F.trim(F.col("Subject")))
)

# Calculate Average number of emails sent per unique sender
total_emails = df_clean.count()
unique_senders = df_clean.select("From").distinct().count()
mean_emails_per_sender = total_emails / unique_senders

print(f"Total emails: {total_emails}")
print(f"Unique senders: {unique_senders}")
print(f"Mean emails per sender: {mean_emails_per_sender:.2f}")

# Top 10 senders by number of emails sent
total_emails_per_sender = df_clean.groupBy("From").count()
top_senders = total_emails_per_sender.orderBy(F.col("count").desc()).limit(10)

print("Top 10 senders by email count:")
top_senders.show(truncate=False)

# Count emails sent between Enron employees
emails_with_recipients = df_clean.filter(F.col("To").isNotNull())

emails_split = emails_with_recipients.withColumn(
    "recipient",
    F.explode(F.split(F.col("To"), ","))
)

emails_split = emails_split.withColumn(
    "recipient",
    F.trim(F.col("recipient"))
)

internal_emails = emails_split.filter(
    (F.col("From").endswith("@enron.com")) &
    (F.col("recipient").endswith("@enron.com"))
)

internal_count = internal_emails.count()
print(f"Internal Enron emails: {internal_count}")

# Daily email volume for 2001
emails_cleaned = df_clean.withColumn(
    "clean_date",
    F.regexp_replace(F.col("Date"), r"^\w{3},\s*|\s*\(.*?\)", "")
)

emails_date = emails_cleaned.withColumn(
    "timestamp",
    F.to_timestamp("clean_date", "d MMM yyyy HH:mm:ss Z")
)

emails_2001 = emails_date.filter(F.year("timestamp") == 2001)

daily_email_count = (
    emails_2001
    .withColumn("email_date", F.to_date("timestamp"))
    .groupBy("email_date")
    .count()
    .orderBy("email_date")
)

print("Daily email counts for 2001:")
daily_email_count.show(truncate=False)
