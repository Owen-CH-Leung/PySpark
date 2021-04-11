def load_from_csv(file_path, spark, schema, no_of_header_row):
    if no_of_header_row == 1:
        if schema == None:
            #Return all df column as String
            df = spark.read.load(
                file_path,
                format="csv",
                header="true",
                inferSchema="false",
                ignoreLeadingWhiteSpace="true",
                ignoreTrailingWhiteSpace="true",
            )
            return df
        elif schema != None:
            #Return all df column based on the passed in schema
            df = spark.read.load(
                file_path,
                format="csv",
                header="true",
                inferSchema="false",
                schema=schema,
                ignoreLeadingWhiteSpace="true",
                ignoreTrailingWhiteSpace="true",
            )
            return df
    elif no_of_header_row == 0:
        #Return all df column as String, without header row
        df = spark.read.load(
            file_path,
            format="csv",
            header="false",
            inferSchema="false",
            schema=schema,
            ignoreLeadingWhiteSpace="true",
            ignoreTrailingWhiteSpace="true",
        )
        return df
