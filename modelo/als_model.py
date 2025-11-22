from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

def entrenar_als(input_path, model_path, recs_path):
    spark = SparkSession.builder \
        .appName("EntrenamientoALS") \
        .getOrCreate()

    print("=== LEYENDO DATASET PREPROCESADO DESDE HDFS ===")
    df = spark.read.parquet(input_path)

    df = df.select("userIndex", "itemIndex", "rating")

    print("=== SPLIT TRAIN/TEST ===")
    train, test = df.randomSplit([0.8, 0.2], seed=42)

    print("=== ENTRENANDO ALS ===")
    als = ALS(
        userCol="userIndex",
        itemCol="itemIndex",
        ratingCol="rating",
        nonnegative=True,
        coldStartStrategy="drop",   # evita NaN en predicciones
        maxIter=10,
        regParam=0.1,
        rank=20                     # tamaño de embedding
    )

    model = als.fit(train)

    print("=== EVALUANDO MODELO ===")
    predictions = model.transform(test)

    evaluator = RegressionEvaluator(
        metricName="rmse", 
        labelCol="rating",
        predictionCol="prediction"
    )

    rmse = evaluator.evaluate(predictions)
    print(f"RMSE DEL MODELO: {rmse}")

    print("=== GUARDANDO MODELO EN HDFS ===")
    model.save(model_path)

    print("=== GENERANDO 10 RECOMENDACIONES POR USUARIO ===")
    user_recs = model.recommendForAllUsers(10)
    user_recs.write.mode("overwrite").parquet(recs_path)

    print("=== PROCESO FINALIZADO ===")
    spark.stop()


if __name__ == "__main__":
    input_path = "hdfs://10.6.101.127:9000/data/proyecto/preprocesado/"
    model_path = "hdfs://10.6.101.127:9000/data/proyecto/modelo_als/"
    recs_path  = "hdfs://10.6.101.127:9000/data/proyecto/recomendaciones/"

    entrenar_als(input_path, model_path, recs_path)
