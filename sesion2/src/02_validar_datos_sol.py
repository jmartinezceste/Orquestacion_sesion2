import sys
import argparse
import json
from pyspark.sql.functions import col, count, isnan, when, to_date, min as spark_min, max as spark_max


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", required=True, help="Ruta de la Delta tabla de entrada")
    parser.add_argument("--output_table", required=True, help="Nombre de la Delta Table destino")
    parser.add_argument("--config_path", required=True, help="Ruta del archivo JSON con la configuración")
    parser.add_argument("--table_name", required=True, help="Nombre de la tabla en la configuración JSON")
    
    args = parser.parse_args()

    df = spark.table(args.input_table)

    with open(args.config_path, 'r') as f:
        config = json.load(f)

    table_config = config.get(args.table_name, {})

    claves_primarias = table_config.get("claves_primarias", [])
    columnas_categoricas = table_config.get("columnas_categoricas", {})
    columnas_numericas = table_config.get("columnas_numericas", {})
    columna_fecha = table_config.get("columna_fecha", "")
    columnas_completas = table_config.get("columnas_completas", [])


    # 0. Seleccionamos las columnas que queremos
    df = df.select([col(c) for c in columnas_completas])

    print(f"Validando DataFrame: {args.input_table}")

    # 1. Nulos en claves primarias
    if claves_primarias:
        for col_name in claves_primarias:
            nulos = df.filter(col(col_name).isNull()).count()
            print(f"Nulos en clave primaria '{col_name}': {nulos}")
    
    # 2. Duplicados por claves primarias
    if claves_primarias:
        dupes = df.groupBy(claves_primarias).count().filter("count > 1").count()
        print(f"Duplicados por clave primaria: {dupes}")
    
    # 3. Rango de columnas numéricas
    for col_name, (min_val, max_val) in columnas_numericas.items():
        stats = df.select(spark_min(col_name), spark_max(col_name)).first()
        if stats:
            print(f"Rango de '{col_name}': {stats[0]} - {stats[1]} (esperado: {min_val}-{max_val})")
    
    # 4. Validación de columna de fecha
    if columna_fecha:
        df_fecha = df.withColumn("fecha_parseada", to_date(col(columna_fecha)))
        nulas = df_fecha.filter(col("fecha_parseada").isNull()).count()
        print(f"Fechas mal formateadas en '{columna_fecha}': {nulas}")

    
    df.write.format("delta").mode("overwrite").saveAsTable(args.output_table)

    print("Validación completada.\n")


if __name__ == "__main__":
    main()