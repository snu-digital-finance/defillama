import polars as pl

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def leave_first_last(
    df: pl.DataFrame, col:str
) -> pl.DataFrame:
    df = df.with_columns([
        pl.arange(0, df.shape[0], eager=True).alias("index")
    ])
    return df.filter(
        (pl.col(col) != pl.col(col).shift(1)) |
        (pl.col(col) != pl.col(col).shift(-1)) |
        (pl.col("index") == 0) | # 첫 번째 행 포함
        (pl.col("index") == (df.shape[0] - 1)) # 마지막 행 포함
    ).drop("index")


def drop_null_rows(
    df: pl.DataFrame
) -> pl.DataFrame:
    condition = pl.lit(False)
    for col in [col for col in df.columns if col not in ['date', 'hourly_date']]:
        condition = condition | pl.col(col).is_not_null()
    return df.filter(condition)

def change_hourly(
    df: pl.DataFrame, col:str="hourly_date"
) -> pl.DataFrame:
    return df.group_by(col).agg([
        pl.col(company).mean().alias(company) 
        for company in df.columns if company != col
    ]).sort(col)

def cast_column_type_without_date(
    df: pl.DataFrame, dtype:object=float, start_colnum:int=1
) -> pl.DataFrame:
    for col in df.columns[start_colnum:]:
        df = df.with_columns(pl.col(col).cast(dtype))
    return df

def cast_datetime(
    df: pl.DataFrame, has_ms=False, col:str="date"
) -> pl.DataFrame:
    return df.with_columns([
        pl.col(col).str.to_datetime(
            format=("%Y-%m-%dT%H:%M:%S.%f" if has_ms else DATE_FORMAT))
    ])

def truncate_datetime(
    df: pl.DataFrame, col:str="date", trun:str="1h"
) -> pl.DataFrame:
    return df.with_columns(pl.col(col).dt.truncate(trun))