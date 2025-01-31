## **Bucketing in Apache Spark**  
Bucketing is a technique used to optimize joins and aggregations by storing data in pre-sorted, fixed-size partitions based on a specified column. This reduces shuffle operations during queries, improving performance.  

### **How Bucketing Works**  
- The data is **physically stored** in buckets based on a column.  
- When performing joins or aggregations, Spark can **read only relevant buckets** instead of scanning the whole dataset.  
- Bucketing works well when two large tables share a common **bucketed column**.

---

## **How to Implement Bucketing in Databricks (Apache Spark)**  

### **Step 1: Write Data into Buckets**
To bucket a DataFrame, use the `.bucketBy()` method when writing a table to a Hive-compatible format (Parquet, ORC, etc.).

```python
# Save the DataFrame as a bucketed table
df.write \
    .mode("overwrite") \
    .format("parquet") \
    .bucketBy(10, "customer_id") \  # 10 buckets based on "customer_id"
    .sortBy("customer_id") \  # Sorting improves query performance
    .saveAsTable("bucketed_customers")
```
✅ **Key Points:**
- This will store the data in **10 buckets** based on `customer_id`.
- `sortBy("customer_id")` ensures sorted data in each bucket, further optimizing queries.
- The table **must** be saved in Hive-compatible formats (Parquet, ORC).

---

### **Step 2: Reading a Bucketed Table**
Once bucketed, you can read the data as follows:

```python
df = spark.read.table("bucketed_customers")
```
This will **not rebalance** the buckets—it simply reads the stored data.

---

### **Step 3: Perform a Join on Bucketed Tables (Without Full Shuffle)**
If two tables are bucketed on the same column and have the **same number of buckets**, Spark can efficiently join them without a full shuffle.

```python
df1 = spark.read.table("bucketed_customers")
df2 = spark.read.table("bucketed_orders")  # Assume bucketed on "customer_id"

# Enable bucketing optimization
spark.conf.set("spark.sql.bucketing.enabled", "true")

# Perform a join (avoiding shuffle)
df_result = df1.join(df2, "customer_id", "inner")
df_result.show()
```
✅ **Why this is Fast?**  
Since both tables are **bucketed on the same column (`customer_id`) with the same number of buckets**, Spark **avoids a full shuffle** and only reads matching buckets.

---

### **Step 4: Bucketing with SQL in Databricks**
If using SQL, you can create a bucketed table like this:

```sql
CREATE TABLE bucketed_customers (
  customer_id STRING,
  name STRING,
  age INT
)
USING PARQUET
CLUSTER BY (customer_id) INTO 10 BUCKETS;
```
This ensures the data is **clustered (bucketed) by `customer_id`**.

---

## **When to Use Bucketing?**
✅ **Best for:**  
- Large datasets that frequently **join on the same key**.  
- Frequent **groupBy aggregations** on a column.  
- Avoiding **shuffle overhead** during joins.

❌ **Not Ideal for:**  
- Small datasets (overhead of bucketing is not worth it).  
- Dynamic keys where the column values have high cardinality.  

---

## **Conclusion**
Bucketing is a powerful optimization technique in Spark to minimize shuffle costs during joins and aggregations. It works best when two large datasets share the same **bucketed column and bucket count**. 🚀