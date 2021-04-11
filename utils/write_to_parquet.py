def df_write_to_parquet(df,mode,path,file_name):
    (
        df
        .write
        .mode("%s"%(mode))
        .parquet("%s/%s.parquet"%(path,file_name))
    )
    
def df_write_to_parquet_by_partition(df,mode,path,file_name,disk_partition_column,max_record_per_file):
    if disk_partition_column != None:
        (
            df
            .write
            .option("maxRecordsPerFile",max_record_per_file)
            .mode("%s"%(mode))
            .partitionBy("%s"%(disk_partition_column))
            .parquet("%s/%s.parquet"%(path,file_name))
        )
    elif disk_partition_column == None:
        (
            df
            .write
            .option("maxRecordsPerFile",max_record_per_file)
            .mode("%s"%(mode))
            .parquet("%s/%s.parquet"%(path,file_name))
        )
        