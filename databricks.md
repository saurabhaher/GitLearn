---
description: https://www.databricks.com/discover/pages/optimize-data-workloads-guide#intro
layout:
  title:
    visible: true
  description:
    visible: true
  tableOfContents:
    visible: true
  outline:
    visible: true
  pagination:
    visible: false
---

# DataBricks

## Optimization

File\
Use Delta Format -  Automatically available in DBR 8 and Above

### <mark style="background-color:orange;">Size</mark>

Ideal file size should be 16MB to 1 GB\
**Use Unity Catalog** - size is auto managed when you use unity catalog\
\
For Non UC delta table\
\
**Optimize** -  compacts the file up to 1 GB  (delta.targetFileSize)\
Use Optimize for table > 1 TB\
Use optimize regularly  ( once a day or once a week)&#x20;

```
OPTIMIZE table_name [WHERE predicate]
[ZORDER BY (col_name1 [, ...] ) ]
```

**Z order**\
Use high cardinality column for z order\
Never use more than 4 column for z order

**Auto Optimize**\
it automatically compact and makes file of 128 mb\
1\. Optimize write - makes 128 mb while writing\
2\. Auto Compact - compact the file to 128 mb

```
delta.autoOptimize.optimizeWrite = true 
delta.autoOptimize.autoCompact = true
```

**Partitioning**\
for Table size > 1 TB and each partition > 1 GB

**File Size tuning**\
(delta.targetFileSize)\
delta.tuneFileSizesForRewrite = true



### <mark style="background-color:orange;">Shuffle</mark>

**Broadcast Hash Join** - \
No shuffling\
broadcast table size < 1GB\
Spark Automatically broadcast table less than 10 mb.\
You can increase this ->

<pre><code><strong>set spark.sql.autoBroadcastJoinThreshold = &#x3C;size in bytes>
</strong>
// Use hints 
SELECT /*+ BROADCAST(t) */ * FROM &#x3C;table-name> t

Select  /* + BROADCASTJOIN(t1) */  * from t1 inner join t2 on t1.key = t2.key

// In pyspark
joinDF = transactionDF.join(broadcast(dimDF), transactionDF.id == dimDF.product_id)

</code></pre>

USE AQE to change join strategies dynamically at runtime. default is 30 mb, it can be changed by

```
set spark.databricks.adaptive.autoBroadcastJoinThreshold = <size in bytes>
```

**Shuffle Hash Join**\
prefer Shuffle Hash Join over Sort Merge Join.

```
set spark.sql.join.preferSortMergeJoin = false
```

**Cost based optimizer**\
Spark SQL can use a Cost-based optimizer (CBO) to improve query plans. This is especially useful for queries with multiple joins. The CBO is enabled by default. \
For CBO its important to collect statistics. \
have practice of collecting and updating statistics regularly.

```
ANALYZE TABLE table_name COMPUTE STATISTICS FROM COLUMNS col1, col2, ...;
```

**Join Reorder**

```
set spark.sql.cbo.joinReorder.enabled = true
```

### <mark style="background-color:orange;">Spilling</mark>

Make each partition - 128 to 200 mb

### <mark style="background-color:orange;">Skew</mark>

Filter Skewed Rows\
Introduce Salting



**Data Skipping and Pruning**\
Predicate Pushdown\
Projection Pushdown\
Partition Pruning - use filter on partitioned column\


**Caching**\
delta cache  - set spark.databricks.io.cache.enabled = true\
spark cache  - cache() and Persist()

**Data Purging**\
vacuum Table

<pre><code><strong>deltaTable.deletedFileRetentionDuration  = “interval 15 days” // Default is 7 days
</strong>vacuum employee_demo retain 72 hours dry run
</code></pre>

