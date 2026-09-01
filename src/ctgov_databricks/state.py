WATERMARKS_TABLE = "workspace.ctgov.watermarks"


def get_high_water_mark(spark, condition: str) -> str | None:
    row = (
        spark.sql(
            f"SELECT last_run_date FROM {WATERMARKS_TABLE} WHERE condition = :condition",
            args={"condition": condition},
        )
        .first()
    )
    return row["last_run_date"] if row else None


def set_high_water_mark(spark, condition: str, run_date: str) -> None:
    spark.sql(
        f"""
        MERGE INTO {WATERMARKS_TABLE} AS t
        USING (SELECT :condition AS condition, :run_date AS last_run_date) AS s
        ON t.condition = s.condition
        WHEN MATCHED THEN UPDATE SET t.last_run_date = s.last_run_date, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (condition, last_run_date, updated_at)
            VALUES (s.condition, s.last_run_date, current_timestamp())
        """,
        args={"condition": condition, "run_date": run_date},
    )
