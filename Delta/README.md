# Spark Delta lake

### Using spark delta we can have type2 data from which latest snapsnot can be derived using 'endDate'

### During the initial load of data all the latest records will have endDate as 'null' (or 31/12/9999)

### During the increamental load, old records will be closed by changing the endDate as latest records startDate (which will the date the record gets created in source/time it gets pushed into the system)

### HashKey -- Column containing hash of primary keys
### HashData -- Column containing hash of remaining data columns (excluding audit columns)

### Using the above two columns delta can be generated

### Below is the sample code which will create type2

```python

dataToInsert = (
        newData.alias("updates")
        .join(oldData.toDF().alias("oldData"), pkey)
        .where("oldData.endDate is null  AND updates.hashData <> oldData.hashData")
    )
    stagedUpdates = dataToInsert.selectExpr("NULL as mergeKey", "updates.*").union(
        newData.alias("updates").selectExpr("updates.{0} as mergeKey".format(pkey), "*")
    )
    oldData.alias("oldData").merge(
        stagedUpdates.alias("staged_updates"),
        "oldData.endDate is null and oldData.{0} = mergeKey".format(pkey),
    ).whenMatchedUpdate(
        condition=" oldData.hashData <> staged_updates.hashData",
        set={"endDate": "staged_updates.startDate"},
    ).whenNotMatchedInsert(
        values={
            "{0}".format(str(col_name)): "staged_updates.{0}".format(str(col_name))
            for col_name in stagedUpdates.columns
            if col_name not in "mergeKey"
        }
    ).execute()

```

