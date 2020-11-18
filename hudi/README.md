### Apache Hudi

### Apache hudi can be used to provide latest snapshot of data on increamental loads

[Sample Guide](https://hudi.apache.org/docs/quick-start-guide.html#pyspark-example)

### Hudi has three options when inserting data into hoodie storage
### Insert,Upsert,Bulk_Insert,Delete,Bootstrap,Insert_Overwrite
### Default : Upsert
[Hudi Write Options Documentation](https://hudi.apache.org/docs/writing_data.html)

### Hudi supports reading data in below modes
### Snapshot,Read_optimized,Incremental
### Default : Snapshot
[Hudi Query Options Documentation](https://hudi.apache.org/docs/querying_data.html)

## Below are the sample options which were tested out

```python
hudi_options_upsert = {
  'hoodie.table.name': tableName,
  'hoodie.datasource.write.recordkey.field': 'hashKey',
  'hoodie.datasource.write.table.name': tableName,
  'hoodie.datasource.write.operation': 'upsert',
  'hoodie.datasource.write.precombine.field': 'createdOn',
  'hoodie.upsert.shuffle.parallelism': 2, 
  'hoodie.insert.shuffle.parallelism': 2
}

```
### Sample initial load 
```python
+----------+--------------------+----------+-------------------+
|customerId|             address|      city|          createdOn|
+----------+--------------------+----------+-------------------+
|         1|   old address for 1|    durham|2020-01-01 02:34:54|
|         1|current address f...|    durham|2020-01-02 02:27:34|
|         2|current address f...|      cary|2020-01-01 07:34:53|
|         3|current address f...|chapel hil|2020-01-01 04:26:15|
+----------+--------------------+----------+-------------------+

```
### After inserting the data using the initial load , data was in below format
```python
+-------------------+--------------------+--------------------+----------------------+--------------------+----------+--------------------+----------+-------------------+--------------------+--------------------+-------------------+-------+
|_hoodie_commit_time|_hoodie_commit_seqno|  _hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name|customerId|             address|      city|          createdOn|             hashKey|            hashData|          startDate|endDate|
+-------------------+--------------------+--------------------+----------------------+--------------------+----------+--------------------+----------+-------------------+--------------------+--------------------+-------------------+-------+
|     20201118135940|  20201118135940_0_1|4e07408562bedb8b6...|               default|662482f5-d0d1-4aa...|         3|current address f...|chapel hil|2020-01-01 04:26:15|4e07408562bedb8b6...|0e6be27212bd4e28e...|2020-01-01 04:26:15|   null|
|     20201118135940|  20201118135940_0_2|6b86b273ff34fce19...|               default|662482f5-d0d1-4aa...|         1|current address f...|    durham|2020-01-02 02:27:34|6b86b273ff34fce19...|46dbc1691eecc26b6...|2020-01-02 02:27:34|   null|
|     20201118135940|  20201118135940_0_3|d4735e3a265e16eee...|               default|662482f5-d0d1-4aa...|         2|current address f...|      cary|2020-01-01 07:34:53|d4735e3a265e16eee...|fa1e24b2a865f8cae...|2020-01-01 07:34:53|   null|
+-------------------+--------------------+--------------------+----------------------+--------------------+----------+--------------------+----------+-------------------+--------------------+--------------------+-------------------+-------+

```
### Sample Increamental load

```python
+----------+--------------------+-------+-------------------+
|customerId|             address|   city|          createdOn|
+----------+--------------------+-------+-------------------+
|         1|   new address for 1| durham|2018-03-03 02:34:65|
|         3|current address f...|   apex|2018-04-04 09:32:23|
|         4|   new address for 4|Raleigh|2018-04-04 04:49:22|
+----------+--------------------+-------+-------------------+
```

### After updating the hoodie data with increamenta load 

```python
+-------------------+--------------------+--------------------+----------------------+--------------------+----------+--------------------+-------+-------------------+--------------------+--------------------+-------------------+-------+
|_hoodie_commit_time|_hoodie_commit_seqno|  _hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name|customerId|             address|   city|          createdOn| 
            hashKey|            hashData|          startDate|endDate|
+-------------------+--------------------+--------------------+----------------------+--------------------+----------+--------------------+-------+-------------------+--------------------+--------------------+-------------------+-------+
|     20201118142545|  20201118142545_0_8|4e07408562bedb8b6...|               default|662482f5-d0d1-4aa...|         3|current address f...|   apex|2018-04-04 09:32:23|4e07408562bedb8b6...|a25d8362438ee44e0...|2018-04-04 09:32:23|   null|
|     20201118142545|  20201118142545_0_9|6b86b273ff34fce19...|               default|662482f5-d0d1-4aa...|         1|   new address for 1| durham|2018-03-03 02:34:65|6b86b273ff34fce19...|99b862ad30cac4bec...|2018-03-03 02:34:65|   null|
|     20201118135940|  20201118135940_0_3|d4735e3a265e16eee...|               default|662482f5-d0d1-4aa...|         2|current address f...|   cary|2020-01-01 07:34:53|d4735e3a265e16eee...|fa1e24b2a865f8cae...|2020-01-01 07:34:53|   null|
|     20201118142545| 20201118142545_0_10|4b227777d4dd1fc61...|               default|662482f5-d0d1-4aa...|         4|   new address for 4|Raleigh|2018-04-04 04:49:22|4b227777d4dd1fc61...|8662a510123fb5c66...|2018-04-04 04:49:22|   null|
+-------------------+--------------------+--------------------+----------------------+--------------------+----------+--------------------+-------+-------------------+--------------------+--------------------+-------------------+-------+

```

