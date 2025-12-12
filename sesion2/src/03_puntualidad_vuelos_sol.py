import argparse
from pyspark.sql.functions import col, avg, count, year, month, when


# ----------------------------
# Vista 1: KPI de puntualidad por mes y modelo de aeronave
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vuelos_table", required=True, help="Ruta de la Delta tabla de los vuelos")
    parser.add_argument("--aeronaves_table", required=True, help="Nombre de la Delta tabla de las aeronaves")
    parser.add_argument("--aeropuertos_table", required=True, help="Nombre de la Delta tabla de los aeropuertos")
    parser.add_argument("--output_table", required=True, help="Nombre de la tabla en la configuración JSON")
    
    args = parser.parse_args()

    df_vuelos = spark.table(args.vuelos_table)
    df_dim_aeronaves = spark.table(args.aeronaves_table)
    df_dim_aeropuertos = spark.table(args.aeropuertos_table)

    df_vuelos_enriquecido = df_vuelos \
        .join(df_dim_aeronaves, "aeronave_id") \
        .join(df_dim_aeropuertos.withColumnRenamed("aeropuerto_id", "origen_id"), "origen_id") \
        .withColumn("anio", year("fecha")) \
        .withColumn("mes", month("fecha"))

    df_kpi_puntualidad = df_vuelos_enriquecido \
        .groupBy("modelo", "pais", "anio", "mes") \
        .agg(
            count("vuelo_id").alias("total_vuelos"),
            count(when(col("estado") == "a_tiempo", True)).alias("vuelos_a_tiempo")
        ) \
        .withColumn("puntualidad_pct", (col("vuelos_a_tiempo") / col("total_vuelos")) * 100)

    df_kpi_puntualidad.write.format("delta").mode("overwrite").saveAsTable(args.output_table)


if __name__ == "__main__":
    main()