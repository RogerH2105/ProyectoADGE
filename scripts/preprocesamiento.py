from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer

def preprocesar_reviews(input_path, output_path):
    spark = SparkSession.builder \
        .appName("PreprocesamientoALS") \
        .getOrCreate()

    print("=== LEYENDO DATASET TABULAR DESDE HDFS (PARQUET) ===")
    df = spark.read.parquet(input_path)

    print("=== COLUMNAS ORIGINALES ===")
    print(df.columns)

    # Renombrar columnas según las claves originales del dataset de Amazon
    # Ejemplo: "reviewerID", "product/productId", "review/score"
    df = df.withColumnRenamed("reviewerID", "user_id") \
           .withColumnRenamed("product/productId", "item_id") \
           .withColumnRenamed("review/score", "rating")

    # Filtrar columnas relevantes
    df2 = df.select("user_id", "item_id", "rating") \
            .dropna(subset=["user_id", "item_id", "rating"]) \
            .withColumn("rating", col("rating").cast("float"))

    print("=== APLICANDO STRING INDEXER ===")
    indexer_user = StringIndexer(inputCol="user_id", outputCol="userIndex")
    indexer_item = StringIndexer(inputCol="item_id", outputCol="itemIndex")

    df_indexed = indexer_user.fit(df2).transform(df2)
    df_indexed = indexer_item.fit(df_indexed).transform(df_indexed)

    print("=== DATAFRAME FINAL LISTO PARA ALS ===")
    df_indexed.show(5)

    print("=== GUARDANDO DATASET PROCESADO EN HDFS ===")
    df_indexed.write.mode("overwrite").parquet(output_path)

    spark.stop()
    print("=== PREPROCESAMIENTO COMPLETADO ===")


if __name__ == "__main__":
    input_path = "hdfs:///10.6.101.127:9000/data/spark/arts_tabular/"
    output_path = "hdfs:///10.6.101.127:9000/data/spark/processed/"

    preprocesar_reviews(input_path, output_path)
