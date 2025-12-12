import pandas as pd
import argparse
from pyspark.sql.functions import input_file_name, current_timestamp

def main():
    ############################################### Hay que cambiar todo
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", required=True, help="Nombre de archivo")
    parser.add_argument("--output_table", required=True, help="Nombre de la Delta Table destino")
    parser.add_argument("--write_mode", required=True, help="Formato de escritura de la Delta Table de destino")
    parser.add_argument("--partition_by", required=True, help="Columnas para particionar la Delta Table destino, separadas por coma")

    args = parser.parse_args()
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(args.input_file)

    # Comprobar si el archivo no tiene filas
    if df.count() == 0:
        raise Exception("El archivo no contiene filas para procesar.")

    partition_columns = args.partition_by.split(",")
    df.write.format("delta").mode(args.write_mode).partitionBy(*partition_columns).saveAsTable(args.output_table)
    print("Ingesta completada.")

if __name__ == "__main__":
    main()