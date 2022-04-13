import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

equipment_day_file = sys.argv[1]
postgres_db = sys.argv[2]
postgres_user = sys.argv[3]
postgres_pwd = sys.argv[4]

####################################
# Read CSV Data
####################################
try:
    print("######################################")
    print("READING DATA")
    print("######################################")


    df_equipment = (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres/airflow")
        .option("dbtable", "public.equipment")
        .option("user", "airflow")
        .option("password", "airflow")
        .load()
    )

    day = (spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema",True)
        .option("timestampFormat", "yyyy-MM-dd")
        .load(equipment_day_file)
    )

    print("######################################")
    print("PROCESSING")
    print("######################################")

    max_date = (
        df_equipment.select(
            F.date_add(F.max(F.col("date")),1).alias("current_date")
        )
    )


    day = (day.alias("day")
        .join(max_date,
            day.date==max_date.current_date,
            how="inner")
        .select("day.*")
    )

    print("######################################")
    print("LOADING POSTGRES TABLES")
    print("######################################")

    (day
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres/airflow")
        .option("dbtable", "public.equipment")
        .option("user", "airflow")
        .option("password", "airflow")
        .mode("append")
        .save()
    )

finally:
    spark.stop()
