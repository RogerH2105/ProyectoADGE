from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer

def preprocesar_reviews(ruta_hdfs_input, ruta_hdfs_output):
    """
    Preprocesa el dataset de reviews almacenado en HDFS:
      - Selecciona las columnas necesarias
      - Filtra valores nulos o inconsistentes
      - Convierte IDs string a índices numéricos (ALS los exige)
      - Guarda el resultado en formato Parquet listo para entrenamiento
    """

    spark = SparkSession.builder \
        .appName("PreprocesamientoALS") \
        .getOrCreate()

    print("=== Leyendo dataset desde HDFS ===")
    df = spark.read.option("header", "true").csv(ruta_hdfs_input)

    print("=== Columnas originales ===")
    print(df.columns)

    # Seleccionar solo lo que ALS necesita
    print("=== Seleccionando columnas relevantes ===")
    df2 = df.select(
        col("reviewerID").alias("user_id"),
        col("product/productId").alias("item_id"),
        col("review/score").alias("rating")
    )

    # Convertir rating a float
    df2 = df2.withColumn("rating", col("rating").cast("float"))

    # Filtrar valores nulos
    df2 = df2.dropna(subset=["user_id", "item_id", "rating"])

    print("=== Aplicando StringIndexer ===")

    # user_id → userIndex
    idx_user = StringIndexer(
        inputCol="user_id",
        outputCol="userIndex"
    )

    # item_id → itemIndex
    idx_item = StringIndexer(
        inputCol="item_id",
        outputCol="itemIndex"
    )

    # Ajustar y transformar
    df_indexed = idx_user.fit(df2).transform(df2)
    df_indexed = idx_item.fit(df_indexed).transform(df_indexed)

    print("=== DataFrame final listo para ALS ===")
    df_indexed.show(5)

    print("=== Guardando preprocesado en HDFS (Parquet) ===")
    df_indexed.write.mode("overwrite").parquet(ruta_hdfs_output)

    spark.stop()
    print("=== PROCESO DE PREPROCESAMIENTO COMPLETADO ===")

# Ejecución
if __name__ == "__main__":
    ruta_input = "hdfs:///10.6.127:9000/data/spark/Arts_tabular.csv"
    ruta_output = "hdfs:///10.6.127:9000/data/spark/preprocesado/"
    preprocesar_reviews(ruta_input, ruta_output)
