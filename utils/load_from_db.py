def load_from_db(db_table_name, config, spark):
    db_table = "(SELECT * FROM %s)" % (db_table_name)
    # Read database table as spark dataframe
    return (
        spark.read.format("jdbc")
        .options(
            url=config["url"],
            driver=config["driver"],
            dbtable=db_table,
            user=config["user"],
            password=config["password"],
            fetchsize=int(50000),
        )
        .load()
    )


def load_from_db_by_partition(db_table_name, config, spark,
                               partitionColumn, lowerBound, upperBound, numPartitions
                              ):
    db_table = "(SELECT * FROM %s)" % (db_table_name)
    # Read database table with partition as spark dataframe, 
    return (
        spark.read.format("jdbc")
        .options(
            url=config["url"],
            driver=config["driver"],
            dbtable=db_table,
            user=config["user"],
            password=config["password"],
            fetchsize=int(50000),
            partitionColumn = partitionColumn,            
            lowerBound = lowerBound, 
            upperBound = upperBound, 
            numPartitions = numPartitions,
        )
        .load()
    )     