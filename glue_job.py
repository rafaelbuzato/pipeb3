import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ====================
# INICIALIZAÇÃO
# ====================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ====================
# CONFIGURAÇÕES
# ====================
S3_INPUT_PATH = "s3://seu-bucket-bovespa/raw/"
S3_OUTPUT_PATH = "s3://seu-bucket-bovespa/refined/"
DATABASE_NAME = "default"
TABLE_NAME = "acoes_bovespa_refined"

print("="*50)
print("INICIANDO PROCESSAMENTO DE DADOS B3")
print("="*50)

# ====================
# 1. LEITURA DOS DADOS
# ====================
print("\n[1/6] Lendo dados brutos do S3...")
df_raw = spark.read.parquet(S3_INPUT_PATH)

print(f"Total de registros lidos: {df_raw.count()}")
print("Schema dos dados brutos:")
df_raw.printSchema()
df_raw.show(5, truncate=False)

# ====================
# 2. REQUISITO 5.A - AGRUPAMENTO E AGREGAÇÃO
# ====================
print("\n[2/6] Aplicando AGREGAÇÕES (Requisito 5.A)...")

df_agregado = df_raw.groupBy(
    "ticker",
    F.to_date("data").alias("data_negociacao")
).agg(
    # SOMA - Volume total
    F.sum("volume").alias("volume_total_dia"),
    
    # CONTAGEM - Quantidade de operações
    F.count("*").alias("qtd_operacoes"),
    
    # MÉDIA - Preços médios
    F.avg("preco_fechamento").alias("preco_medio_fechamento"),
    F.avg("preco_abertura").alias("preco_medio_abertura"),
    
    # SUMARIZAÇÃO - Estatísticas
    F.max("preco_max").alias("preco_maximo_dia"),
    F.min("preco_min").alias("preco_minimo_dia"),
    F.first("preco_abertura").alias("preco_abertura_dia"),
    F.last("preco_fechamento").alias("preco_fechamento_dia"),
    
    # Volatilidade
    F.stddev("preco_fechamento").alias("volatilidade_preco")
)

print(f"Registros após agregação: {df_agregado.count()}")
df_agregado.show(5, truncate=False)

# ====================
# 3. REQUISITO 5.B - RENOMEAR COLUNAS
# ====================
print("\n[3/6] Renomeando colunas (Requisito 5.B)...")

# Renomear pelo menos 2 colunas (além das de agrupamento)
df_renomeado = df_agregado \
    .withColumnRenamed("preco_medio_fechamento", "preco_medio_ajustado") \
    .withColumnRenamed("volume_total_dia", "volume_financeiro_negociado") \
    .withColumnRenamed("preco_maximo_dia", "maxima_diaria") \
    .withColumnRenamed("preco_minimo_dia", "minima_diaria") \
    .withColumnRenamed("qtd_operacoes", "total_negociacoes") \
    .withColumnRenamed("volatilidade_preco", "desvio_padrao_preco")

print("Colunas renomeadas com sucesso!")
df_renomeado.printSchema()

# ====================
# 4. REQUISITO 5.C - CÁLCULOS TEMPORAIS
# ====================
print("\n[4/6] Aplicando cálculos temporais (Requisito 5.C)...")

# Window specifications
window_spec = Window.partitionBy("ticker").orderBy("data_negociacao")
window_7d = window_spec.rowsBetween(-6, 0)
window_21d = window_spec.rowsBetween(-20, 0)
window_30d = window_spec.rowsBetween(-29, 0)

# MÉDIAS MÓVEIS
print("  -> Calculando médias móveis...")
df_final = df_renomeado \
    .withColumn("media_movel_7d", 
                F.avg("preco_medio_ajustado").over(window_7d)) \
    .withColumn("media_movel_21d", 
                F.avg("preco_medio_ajustado").over(window_21d))

# DIFERENÇAS ENTRE PERÍODOS
print("  -> Calculando variações...")
df_final = df_final \
    .withColumn("preco_dia_anterior", 
                F.lag("preco_medio_ajustado", 1).over(window_spec)) \
    .withColumn("variacao_diaria_absoluta", 
                F.col("preco_medio_ajustado") - F.col("preco_dia_anterior")) \
    .withColumn("variacao_diaria_percentual", 
                F.when(F.col("preco_dia_anterior").isNotNull(),
                    ((F.col("preco_medio_ajustado") - F.col("preco_dia_anterior")) / 
                     F.col("preco_dia_anterior") * 100)
                ).otherwise(0))

# VALORES EXTREMOS NO PERÍODO
print("  -> Calculando valores extremos...")
df_final = df_final \
    .withColumn("maxima_30d", 
                F.max("preco_medio_ajustado").over(window_30d)) \
    .withColumn("minima_30d", 
                F.min("preco_medio_ajustado").over(window_30d)) \
    .withColumn("amplitude_30d", 
                F.col("maxima_30d") - F.col("minima_30d"))

# DIAS DESDE INÍCIO
print("  -> Calculando dias desde início...")
df_final = df_final \
    .withColumn("dias_desde_inicio", 
                F.datediff(
                    F.col("data_negociacao"), 
                    F.min("data_negociacao").over(Window.partitionBy("ticker"))
                ))

# COMPONENTES DE DATA
print("  -> Extraindo componentes de data...")
df_final = df_final \
    .withColumn("ano", F.year("data_negociacao")) \
    .withColumn("mes", F.month("data_negociacao")) \
    .withColumn("dia", F.dayofmonth("data_negociacao")) \
    .withColumn("trimestre", F.quarter("data_negociacao")) \
    .withColumn("dia_semana", F.dayofweek("data_negociacao"))

# METADADOS
df_final = df_final \
    .withColumn("data_processamento", F.current_timestamp()) \
    .withColumn("versao_processamento", F.lit("1.0"))

print("Transformações temporais concluídas!")
print(f"Total de colunas no DataFrame final: {len(df_final.columns)}")
df_final.show(5, truncate=False)

# ====================
# 5. SALVAR DADOS REFINADOS (Requisito 6)
# ====================
print("\n[5/6] Salvando dados refinados no S3...")

df_final.write \
    .mode("overwrite") \
    .partitionBy("ano", "mes", "ticker") \
    .parquet(S3_OUTPUT_PATH)

print(f"✓ Dados salvos em: {S3_OUTPUT_PATH}")
print(f"✓ Particionado por: ano, mes, ticker")

# ====================
# 6. CATALOGAR NO GLUE (Requisito 7)
# ====================
print("\n[6/6] Catalogando dados no Glue Catalog...")

# Criar/atualizar tabela no Glue Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME}
    USING PARQUET
    LOCATION '{S3_OUTPUT_PATH}'
""")

print(f"✓ Tabela {TABLE_NAME} criada/atualizada no database {DATABASE_NAME}")

# Atualizar partições
spark.sql(f"MSCK REPAIR TABLE {DATABASE_NAME}.{TABLE_NAME}")
print("✓ Partições atualizadas")

# ====================
# ESTATÍSTICAS FINAIS
# ====================
print("\n" + "="*50)
print("PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
print("="*50)
print(f"Total de registros processados: {df_final.count()}")
print(f"Total de tickers únicos: {df_final.select('ticker').distinct().count()}")
print(f"Período de dados: {df_final.agg(F.min('data_negociacao'), F.max('data_negociacao')).collect()[0]}")
print("="*50)

job.commit()