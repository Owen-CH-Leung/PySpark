import pyspark.sql.functions as F

def transpose_df(df, by):
    '''
    Parameters
    ----------
    df : Spark DataFrame
    by : list of Columns that do not require transpose

    Returns
    -------
    Transposed Spark DataFrame
    '''
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))

    # Spark SQL supports only homogeneous columns
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # Create and explode an array of (column_name, column_value) structs
    kvs = F.explode(
        F.array(
            [
                F.struct(
                    F.lit(c).alias("COLUMN_NAME"),
                    F.col(c).alias("COLUMN_VALUE"),
                )
                for c in cols
            ]
        )
    ).alias("kvs")

    return df.select(by + [kvs]).select(
        by + ["kvs.COLUMN_NAME", "kvs.COLUMN_VALUE"]
    )