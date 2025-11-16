from pyspark.sql import SparkSession

def parse_line_pairs(lines):
    """
    Recibe un RDD de líneas crudas y produce pares clave:valor agrupados por review.
    """
    reviews = []
    entry = {}

    for line in lines:
        line = line.strip()
        if not line:
            if entry:
                reviews.append(entry)
                entry = {}
            continue

        colon = line.find(':')
        if colon != -1:
            key = line[:colon]
            value = line[colon + 2:]
            entry[key] = value

    if entry:
        reviews.append(entry)

    return reviews


def parse_raw_amazon_hdfs(input_path, output_path):
    spark = SparkSession.builder \
        .appName("ParseAmazonRawToParquet") \
        .getOrCreate()

    print("=== LEYENDO DATASET CRUDO DESDE HDFS ===")
    rdd = spark.sparkContext.textFile(input_path)

    # Convertir RDD de líneas → RDD de estructuras tipo JSON por registro
    parsed_rdd = rdd.mapPartitions(parse_line_pairs)

    # Aplanar (porque mapPartitions devuelve listas)
    flat_rdd = parsed_rdd.flatMap(lambda x: x)

    print(" CONVIRTIENDO RDD A DATAFRAME")
    df = spark.createDataFrame(flat_rdd)

    print("COLUMNAS ENCONTRADAS ")
    print(df.columns)

    print(" GUARDANDO FORMATO TABULAR EN PARQUET")
    df.write.mode("overwrite").parquet(output_path)

    print("PARSEO COMPLETADO ")
    spark.stop()


if __name__ == "__main__":
    input_path = "hdfs:///10.6.101.127:9000/data/raw/Arts.txt"
    output_path = "hdfs:///10.6.101.127:9000/data/spark/arts_tabular/"

    parse_raw_amazon_hdfs(input_path, output_path)
