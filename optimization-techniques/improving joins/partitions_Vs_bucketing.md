### **Difference Between Partitioning and Bucketing in Apache Spark**  

| Feature           | **Partitioning (`PARTITION BY`)** | **Bucketing (`BUCKET BY`)** |
|------------------|---------------------------------|-----------------------------|
| **Concept**      | Divides data into separate physical subdirectories based on a column value. | Divides data into fixed-number of buckets based on the column's hash value. |
| **Storage**      | Each partition is stored as a separate folder. | Buckets are stored as fixed-sized files within partitions. |
| **Number of Partitions** | Dynamic (depends on unique column values). | Fixed (set manually). |
| **Query Performance** | Pruning helps filter data faster, avoiding full table scans. | Optimizes joins and aggregations by pre-grouping similar data in fixed-size buckets. |
| **Shuffle Optimization** | Reduces shuffle in `WHERE` queries (partition pruning). | Reduces shuffle in joins and aggregations. |
| **Joins & Aggregations** | Does not optimize joins between partitioned tables. | Helps optimize joins if tables have the same bucket column and count. |
| **Best for** | Large datasets that need fast filtering (`WHERE` queries). | Large datasets that need optimized joins & aggregations. |
| **Example** | `df.write.partitionBy("country")` | `df.write.bucketBy(10, "customer_id")` |

---

## **Examples in Databricks (PySpark)**  

### **1️⃣ Partitioning Example (Efficient Filtering)**
```python
df.write \
    .mode("overwrite") \
    .partitionBy("country") \  # Creates separate folders for each country
    .parquet("/mnt/data/partitioned_customers")
```
**Storage Structure:**
```
/mnt/data/partitioned_customers/country=USA/part-0000.parquet
/mnt/data/partitioned_customers/country=UK/part-0001.parquet
```
✅ **Query Optimization:**  
Spark **skips irrelevant partitions** when filtering on `country`:
```python
df = spark.read.parquet("/mnt/data/partitioned_customers")
df_filtered = df.filter(df["country"] == "USA")  # Loads only the 'USA' partition
```

---

### **2️⃣ Bucketing Example (Efficient Joins & Aggregations)**
```python
df.write \
    .mode("overwrite") \
    .bucketBy(10, "customer_id") \  # Fixed 10 buckets based on `customer_id`
    .sortBy("customer_id") \
    .format("parquet") \
    .saveAsTable("bucketed_customers")
```
**Optimized Join:**
```python
df1 = spark.read.table("bucketed_customers")
df2 = spark.read.table("bucketed_orders")  # Assume also bucketed by `customer_id`

spark.conf.set("spark.sql.bucketing.enabled", "true")

df_result = df1.join(df2, "customer_id", "inner")  # Avoids full shuffle
```
✅ **Why?**  
Since both tables are bucketed on `customer_id`, Spark **reads only relevant buckets** without shuffling.

---

## **Key Takeaways**
- **Use Partitioning (`partitionBy`)** for **efficient filtering** (`WHERE` queries).  
- **Use Bucketing (`bucketBy`)** for **efficient joins & aggregations**.  
- **You can combine both** for even better performance! 🚀  

