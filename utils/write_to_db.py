def df_to_db_insert_only(db_table_name, config, df):
    #db_table_name consists of (schema_name.table_name)
    #Append database table using spark dataframe as source
    df.write.format("jdbc").mode("append").options(
        url=config["url"],
        driver=config["driver"],
        dbtable=db_table_name,
        user=config["user"],
        password=config["password"],
        batchsize=50000,
    ).save()


def df_to_db_overwrite(db_table_name, config, df):
    #db_table_name consists of (schema_name.table_name)
    #Append database table using spark dataframe as source
    df.write.format("jdbc").mode("overwrite").options(
        url=config["url"],
        driver=config["driver"],
        dbtable=db_table_name,
        user=config["user"],
        password=config["password"],
        batchsize=50000,
        truncate=True,
    ).save()


def df_to_db_insert_by_partition(db_table_name, config, df,
                                 partitionColumn, lowerBound, upperBound, numPartitions
                                ):
    #db_table_name consists of (schema_name.table_name)
    #Append database table using spark dataframe as source
    df.write.format("jdbc").mode("append").options(
        url=config["url"],
        driver=config["driver"],
        dbtable=db_table_name,
        user=config["user"],
        password=config["password"],
        partitionColumn = partitionColumn,
        lowerBound = lowerBound, 
        upperBound = upperBound, 
        numPartitions = numPartitions,
        batchsize=50000,
    ).save()
