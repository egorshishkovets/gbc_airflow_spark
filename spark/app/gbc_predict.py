import sys
import pickle
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import pandas_udf
import pyspark.sql.types as T

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

model_path = sys.argv[1]
postgres_db = sys.argv[2]
postgres_user = sys.argv[3]
postgres_pwd = sys.argv[4]

id_column = "id"
order_column = "date"
windowSpec = (Window
              .partitionBy(id_column)
              .orderBy(order_column)
)

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

    max_date = (
        df_equipment.select(
            F.max(F.col("date")).alias("current_date")
        )
    )

    print("######################################")
    print("PREPARE DATA")
    print("######################################")

    for col in df_equipment.columns[1:-1]:
        for i in range (1,4):
            df_equipment = df_equipment.withColumn(f"{col}_{i}",F.lag(col,i).over(windowSpec))

            if i == 1:
                prev_col = col
            else: 
                prev_col =f"{col}_{i-1}"

            df_equipment = df_equipment.withColumn(f"{col}_{i-1}_to_{i}", F.col(f"{col}_{i}") - F.col(prev_col))

        df_equipment = df_equipment.drop(*[f"{col}_{i}" if i!=0 else col for i in range(0,4)])

    df_equipment = (df_equipment.alias("equ")
        .join(max_date,
            df_equipment.date==max_date.current_date,
            how="inner")
        .select("equ.*")
    )

    print("######################################")
    print("SCORE DATA")
    print("######################################")

    model = pickle.load(open(model_path, 'rb'))
    spark.sparkContext.broadcast(model)

    schema = T.StructType([T.StructField('id', T.LongType(), False),
                        T.StructField('date', T.DateType(), False),
                        T.StructField('predict', T.FloatType(), False)
                        ])

    @pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
    def predict_repair(df: pd.DataFrame):
        equipment_id = df['id']
        date = df['date']
        features = df[df.columns[2:]]

        predictions = model.predict(features)

        result = pd.DataFrame({'id': equipment_id,
                            'date': date,
                            'predict': predictions})
        return result


    predictions = df_equipment.groupby('id').apply(predict_repair)

    print("######################################")
    print("SAVE DATA")
    print("######################################")

    (predictions
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres/airflow")
        .option("dbtable", "public.repair_model_stat")
        .option("user", "airflow")
        .option("password", "airflow")
        .mode("append")
        .save()
    )
finally:
    spark.stop()