import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Inicializacao
args = getResolvedOptions(sys.argv, ['JOB_NAME', '--bucket', '--date'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuracoes
BUCKET = args['bucket']
DATE = args['date']
S3_INPUT_PATH = f"s3://{BUCKET}/raw/"
S3_OUTPUT_PATH = f"s3://{BUCKET}/refined/"
DATABASE_NAME = "b3_database"
TABLE_NAME = "acoes_b3_refined"

print("="*60)
print("GLUE JOB - B3 PIPELINE")
print("="*60)
print(f"Bucket: {BUCKET}")
print(f"Input: {S3_INPUT_PATH}")
print(f"Output: {S3_OUTPUT_PATH}")
print("="*60)

# 1. Leitura
print("\n[1/6] Lendo dados brutos...")
df_raw = spark.read.parquet(S3_INPUT_PATH)
print(f"Total registros: {df_raw.count()}")

# 2. Agregacao (Requisito 5.A)
print("\n[2/6] Aplicando agregacoes...")
df_agregado = df_raw.groupBy("ticker", F.to_date("data").alias("data_negociacao")).agg(
    F.sum("volume").alias("volume_total_dia"),
    F.count("*").alias("qtd_operacoes"),
    F.avg("fechamento").alias("preco_medio_fechamento"),
    F.max("maxima").alias("preco_maximo_dia"),
    F.min("minima").alias("preco_minimo_dia")
)

# 3. Renomear (Requisito 5.B)
print("\n[3/6] Renomeando colunas...")
df_renomeado = df_agregado \
    .withColumnRenamed("preco_medio_fechamento", "preco_medio_ajustado") \
    .withColumnRenamed("volume_total_dia", "volume_financeiro_negociado") \
    .withColumnRenamed("preco_maximo_dia", "maxima_diaria") \
    .withColumnRenamed("preco_minimo_dia", "minima_diaria")

# 4. Calculos temporais (Requisito 5.C)
print("\n[4/6] Aplicando calculos temporais...")
window_spec = Window.partitionBy("ticker").orderBy("data_negociacao")
window_7d = window_spec.rowsBetween(-6, 0)
window_21d = window_spec.rowsBetween(-20, 0)

df_final = df_renomeado \
    .withColumn("media_movel_7d", F.avg("preco_medio_ajustado").over(window_7d)) \
    .withColumn("media_movel_21d", F.avg("preco_medio_ajustado").over(window_21d)) \
    .withColumn("preco_dia_anterior", F.lag("preco_medio_ajustado", 1).over(window_spec)) \
    .withColumn("variacao_diaria_pct", 
                F.when(F.col("preco_dia_anterior").isNotNull(),
                    ((F.col("preco_medio_ajustado") - F.col("preco_dia_anterior")) / 
                     F.col("preco_dia_anterior") * 100)).otherwise(0)) \
    .withColumn("dias_desde_inicio", 
                F.datediff(F.col("data_negociacao"), 
                          F.min("data_negociacao").over(Window.partitionBy("ticker")))) \
    .withColumn("ano", F.year("data_negociacao")) \
    .withColumn("mes", F.month("data_negociacao")) \
    .withColumn("data_processamento", F.current_timestamp())

# 5. Salvar (Requisito 6)
print("\n[5/6] Salvando dados refinados...")
df_final.write.mode("overwrite").partitionBy("ano", "mes", "ticker").parquet(S3_OUTPUT_PATH)
print(f"Dados salvos: {S3_OUTPUT_PATH}")

# 6. Catalogar (Requisito 7)
print("\n[6/6] Catalogando no Glue...")
spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME} USING PARQUET LOCATION '{S3_OUTPUT_PATH}'")
spark.sql(f"MSCK REPAIR TABLE {DATABASE_NAME}.{TABLE_NAME}")
print(f"Tabela catalogada: {DATABASE_NAME}.{TABLE_NAME}")

print("\n" + "="*60)
print("PROCESSAMENTO CONCLUIDO!")
print("="*60)
job.commit()