import argparse
from pyspark.sql.functions import col, avg, count, year, month

# ----------------------------
# Vista 2: Costo promedio de mantenimiento por tipo y modelo de aeronave
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aeronaves_table", required=True, help="Nombre de la Delta tabla de las aeronaves")
    parser.add_argument("--mantenimiento_table", required=True, help="Nombre de la Delta table de los estados de los vuelos")
    parser.add_argument("--output_table", required=True, help="Nombre de la tabla en la configuración JSON")
    args = parser.parse_args()

    df_dim_aeronaves = spark.table(args.aeronaves_table)
    df_mantenimientos = spark.table(args.mantenimiento_table)

    df_mantenimiento_enriquecido = df_mantenimientos \
        .join(df_dim_aeronaves, "aeronave_id") \
        .withColumn("anio", year("fecha")) \
        .withColumn("mes", month("fecha"))

    df_kpi_mantenimiento = df_mantenimiento_enriquecido \
        .groupBy("aeronave_id", "modelo", "anio", "mes") \
        .agg(
            count("mantenimiento_id").alias("total_mantenimientos"),
            avg("costo_usd").alias("costo_promedio_usd"),
            avg("duracion_hr").alias("duracion_promedio_hr")
        )

    df_kpi_mantenimiento.write.format("delta").mode("overwrite").saveAsTable(args.output_table)


if __name__ == "__main__":
    main()