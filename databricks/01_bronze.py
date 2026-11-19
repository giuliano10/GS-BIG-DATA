# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ingestão incremental com Auto Loader
# MAGIC Lê os arquivos que subiram ao Volume `chegada/` e grava o dado **bruto**, acrescido de
# MAGIC `arquivo_origem` e `carregado_em`.
# MAGIC
# MAGIC * O **Auto Loader** guarda num *checkpoint* quais arquivos já leu: no 2º lote só entram os arquivos novos.
# MAGIC * Rodar o notebook de novo sem arquivos novos **não duplica nada** (0 linhas novas).
# MAGIC * As contagens antes e depois de cada carga ficam em `workspace.gs.controle_cargas`.

# COMMAND ----------

import uuid
from pyspark.sql import functions as F

BASE = "/Volumes/workspace/gs/chegada"
CTRL = f"{BASE}/_controle"
EXECUCAO = str(uuid.uuid4())[:8]

FONTES = {
    # tabela bronze           pasta      formato  opções do leitor
    "workspace.gs.bronze_sp156":   ("sp156",   "csv",  {"header": "true", "multiLine": "true", "escape": '"'}),
    "workspace.gs.bronze_clima":   ("clima",   "json", {}),
    "workspace.gs.bronze_relatos": ("relatos", "json", {}),
}


def contar(tabela: str) -> int:
    return spark.table(tabela).count() if spark.catalog.tableExists(tabela) else 0


def carregar(tabela: str, pasta: str, formato: str, opcoes: dict) -> dict:
    antes = contar(tabela)
    leitor = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.schemaLocation", f"{CTRL}/schema_{pasta}")
        .option("cloudFiles.inferColumnTypes", "false")  # Bronze guarda tudo como veio (texto)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
    )
    for chave, valor in opcoes.items():
        leitor = leitor.option(chave, valor)

    consulta = (
        leitor.load(f"{BASE}/{pasta}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream.option("checkpointLocation", f"{CTRL}/checkpoint_{pasta}")
        .trigger(availableNow=True)
        .toTable(tabela)
    )
    consulta.awaitTermination()

    depois = contar(tabela)
    arquivos = spark.table(tabela).select("arquivo_origem").distinct().count()
    return {"execucao_id": EXECUCAO, "tabela": tabela, "linhas_antes": antes,
            "linhas_depois": depois, "linhas_novas": depois - antes, "arquivos_total": arquivos}


resultados = [carregar(t, *cfg) for t, cfg in FONTES.items()]
(
    spark.createDataFrame(resultados)
    .withColumn("executado_em", F.current_timestamp())
    .select("execucao_id", "tabela", "linhas_antes", "linhas_depois", "linhas_novas",
            "arquivos_total", "executado_em")
    .write.mode("append").saveAsTable("workspace.gs.controle_cargas")
)
display(spark.createDataFrame(resultados))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Histórico de cargas (print para o relatório: 1º lote, 2º lote e 2ª execução sem arquivos novos)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT execucao_id, tabela, linhas_antes, linhas_depois, linhas_novas, arquivos_total, executado_em
# MAGIC FROM workspace.gs.controle_cargas
# MAGIC ORDER BY executado_em, tabela;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- quantos arquivos e linhas vieram de cada arquivo de coleta
# MAGIC SELECT 'sp156' AS fonte, regexp_extract(arquivo_origem, '[^/]+$', 0) AS arquivo, count(*) AS linhas FROM workspace.gs.bronze_sp156 GROUP BY 1, 2
# MAGIC UNION ALL
# MAGIC SELECT 'clima', regexp_extract(arquivo_origem, '[^/]+$', 0), count(*) FROM workspace.gs.bronze_clima GROUP BY 1, 2
# MAGIC UNION ALL
# MAGIC SELECT 'relatos', regexp_extract(arquivo_origem, '[^/]+$', 0), count(*) FROM workspace.gs.bronze_relatos GROUP BY 1, 2
# MAGIC ORDER BY fonte, arquivo;
