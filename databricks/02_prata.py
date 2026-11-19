# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata: dado tratado
# MAGIC Para cada fonte:
# MAGIC 1. **Tipos corretos**: textos da Bronze viram datas, números e booleanos; o JSON aninhado é aberto.
# MAGIC 2. **Nulos e inválidos tratados**: textos padronizados e nulos com valor padrão quando faz sentido.
# MAGIC 3. **Regras de qualidade**: o que falha vai para `prata_quarentena`, e o total barrado por regra vai para `controle_qualidade`.
# MAGIC 4. **MERGE pela chave**: atualiza o que já existe e insere o que é novo, então rodar de novo não duplica.

# COMMAND ----------

import uuid
from pyspark.sql import functions as F, Window
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, StringType, StructField,
                               StructType)

EXECUCAO = str(uuid.uuid4())[:8]
qualidade = []  # linhas para controle_qualidade

ACENTOS, SEM_ACENTOS = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇáàâãäéèêëíìîïóòôõöúùûüç", "AAAAAEEEEIIIIOOOOOUUUUCaaaaaeeeeiiiiooooouuuuc"


def chave_texto(col):
    """Padroniza nomes para comparar: sem acento, maiúsculo, só letras."""
    return F.regexp_replace(F.upper(F.translate(col, ACENTOS, SEM_ACENTOS)), "[^A-Z]", "")


def data_flex(col):
    """Converte texto em timestamp aceitando os formatos que aparecem nos dados abertos."""
    # os arquivos do SP156 de 2026 usam o formato americano (MM/dd/yyyy): ex. o 3º trimestre tem 07/01 a 09/30
    formatos = ["MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy HH:mm", "MM/dd/yyyy",
                "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd"]
    return F.coalesce(*[F.try_to_timestamp(F.col(col), F.lit(f)) for f in formatos])


def aplicar_regras(df, fonte: str, chave: str, regras: list):
    """regras = [(codigo, descricao, condicao_de_falha)]. Retorna só os registros aprovados."""
    avaliados = df.count()
    df = df.withColumn("_falha", F.lit(None).cast("string"))
    for codigo, descricao, falha in regras:
        df = df.withColumn("_falha", F.when(F.col("_falha").isNull() & falha, F.lit(codigo))
                                      .otherwise(F.col("_falha")))
    contagem = {r["_falha"]: r["count"] for r in df.groupBy("_falha").count().collect()}
    for codigo, descricao, _ in regras:
        qualidade.append((EXECUCAO, fonte, codigo, descricao, avaliados, contagem.get(codigo, 0)))

    barrados = df.filter(F.col("_falha").isNotNull())
    (barrados.select(F.lit(EXECUCAO).alias("execucao_id"), F.lit(fonte).alias("fonte"),
                     F.col("_falha").alias("regra"), F.col(chave).cast("string").alias("chave"),
                     F.to_json(F.struct(*[c for c in df.columns if c != "_falha"])).alias("registro"),
                     F.current_timestamp().alias("executado_em"))
             .write.mode("append").saveAsTable("workspace.gs.prata_quarentena"))
    return df.filter(F.col("_falha").isNull()).drop("_falha")


def como_json(tabela: str, coluna: str):
    """Na Bronze o JSON aninhado fica como texto; se o Spark tiver inferido struct, converte de volta para texto."""
    tipo = dict(spark.table(f"workspace.gs.{tabela}").dtypes)[coluna]
    return F.col(coluna) if tipo == "string" else F.to_json(F.col(coluna))


def ultimo_por_chave(df, chaves: list):
    """Se a mesma chave veio em vários arquivos, fica a versão mais recente (MERGE exige 1 linha por chave)."""
    w = Window.partitionBy(*chaves).orderBy(F.col("carregado_em").desc(), F.col("arquivo_origem").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def merge(df, tabela: str, chaves: list):
    if not spark.catalog.tableExists(tabela):
        df.limit(0).write.saveAsTable(tabela)
    df.createOrReplaceTempView("lote_prata")
    condicao = " AND ".join(f"alvo.{c} = novo.{c}" for c in chaves)
    spark.sql(f"""
        MERGE INTO {tabela} AS alvo
        USING lote_prata AS novo
        ON {condicao}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"{tabela}: {spark.table(tabela).count()} linhas após o MERGE")


ref = spark.table("workspace.gs.ref_subprefeitura").withColumn("ref_chave", chave_texto(F.col("subprefeitura")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## SP156: solicitações de calçada

# COMMAND ----------

b = spark.table("workspace.gs.bronze_sp156")
cols = set(b.columns)
c = lambda nome: F.col(nome) if nome in cols else F.lit(None).cast("string")  # noqa: E731

# o nome das colunas mudou ao longo dos anos ("Canal" / "Meio de comunicação", "Data do parecer" / "Data de finalização")
canal_txt = F.coalesce(c("canal"), c("meio_de_comunicacao"))
fim_txt = "data_de_finalizacao" if "data_de_finalizacao" in cols else "data_do_parecer"

# os dados abertos não trazem número de protocolo: a chave é um hash dos campos que identificam a solicitação
if "protocolo" in cols:
    chave_sp156 = F.col("protocolo").cast("string")
else:
    chave_sp156 = F.sha2(F.concat_ws("|", c("data_de_abertura"), canal_txt, c("servico"), c("logradouro"),
                                     c("numero"), c("cep"), c("subprefeitura"), c("distrito")), 256)

# a coluna se chama "subprefeitura" nos arquivos antigos e "prefeitura_operacional" nos de 2026
sub_txt = F.regexp_replace(
    F.regexp_replace(chave_texto(F.coalesce(c("subprefeitura"), c("prefeitura_operacional"), F.lit(""))),
                     "^SUBPREFEITURA", ""),
    "LIMAO", "")  # "CASA VERDE-LIMAO-CACHOEIRINHA" -> Casa Verde/Cachoeirinha

sp = (
    b.select(
        chave_sp156.alias("id_solicitacao"),
        F.to_date(data_flex("data_de_abertura")).alias("data_abertura"),
        (F.to_date(data_flex(fim_txt)) if fim_txt in cols else F.lit(None).cast("date")).alias("data_parecer"),
        F.initcap(F.trim(canal_txt)).alias("canal"),
        F.trim(c("orgao")).alias("orgao"),
        F.trim(c("tema")).alias("tema"),
        F.trim(c("assunto")).alias("assunto"),
        F.coalesce(F.trim(c("servico")), F.trim(c("assunto")), F.lit("Não informado")).alias("servico"),
        F.upper(F.trim(F.coalesce(c("status_da_solicitacao"), c("status"), F.lit("SEM STATUS")))).alias("status"),
        F.initcap(F.trim(c("distrito"))).alias("distrito"),
        sub_txt.alias("sub_chave"),
        F.expr("try_cast(replace(latitude, ',', '.') AS DOUBLE)").alias("latitude") if "latitude" in cols
        else F.lit(None).cast("double").alias("latitude"),
        F.expr("try_cast(replace(longitude, ',', '.') AS DOUBLE)").alias("longitude") if "longitude" in cols
        else F.lit(None).cast("double").alias("longitude"),
        "arquivo_origem", "carregado_em",
    )
)
# casa o nome da subprefeitura com a tabela de referência (igual ou começando igual: "SAO MIGUEL" -> "SAO MIGUEL PAULISTA")
sp = (
    sp.join(F.broadcast(ref.select("codigo", "ref_chave")),
            (F.col("sub_chave") != "") & ((F.col("ref_chave") == F.col("sub_chave"))
             | ((F.length("sub_chave") >= 5) & F.col("ref_chave").startswith(F.col("sub_chave")))
             | ((F.length("ref_chave") >= 5) & F.col("sub_chave").startswith(F.col("ref_chave")))),
            "left")
    .withColumn("_rn", F.row_number().over(Window.partitionBy("id_solicitacao", "arquivo_origem")
                                           .orderBy(F.length("ref_chave").desc())))
    .filter("_rn = 1")
    .withColumnRenamed("codigo", "subprefeitura_codigo")
    .withColumn("resolvida", F.col("status").contains("FINALIZ"))
    .withColumn("dias_atendimento",
                F.when(F.col("data_parecer").isNotNull(), F.datediff("data_parecer", "data_abertura")))
    .drop("_rn", "ref_chave", "sub_chave")
)

sp_ok = aplicar_regras(sp, "sp156", "id_solicitacao", [
    ("SP156_R1", "Data de abertura ausente ou em formato inválido",
     F.col("data_abertura").isNull()),
    ("SP156_R2", "Data incoerente: abertura no futuro ou parecer antes da abertura",
     (F.col("data_abertura") > F.current_date()) | (F.col("dias_atendimento") < 0)),
    ("SP156_R3", "Subprefeitura não identificada",
     F.col("subprefeitura_codigo").isNull()),
])
merge(ultimo_por_chave(sp_ok, ["id_solicitacao"]), "workspace.gs.prata_sp156", ["id_solicitacao"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clima (Open-Meteo): listas paralelas viram uma linha por subprefeitura e dia (`arrays_zip` + `explode`)

# COMMAND ----------

esquema_daily = StructType([
    StructField("time", ArrayType(StringType())),
    StructField("precipitation_sum", ArrayType(DoubleType())),
    StructField("temperature_2m_max", ArrayType(DoubleType())),
    StructField("temperature_2m_min", ArrayType(DoubleType())),
])

cl = (
    spark.table("workspace.gs.bronze_clima")
    .withColumn("d", F.from_json(como_json("bronze_clima", "daily"), esquema_daily))
    .select("subprefeitura_codigo", "arquivo_origem", "carregado_em",
            F.col("d.time").alias("time"), F.col("d.precipitation_sum").alias("precipitation_sum"),
            F.col("d.temperature_2m_max").alias("temperature_2m_max"),
            F.col("d.temperature_2m_min").alias("temperature_2m_min"))
    # O Open-Meteo devolve listas paralelas: arrays_zip junta as listas e explode gera uma linha por dia
    .withColumn("z", F.explode(F.arrays_zip("time", "precipitation_sum",
                                            "temperature_2m_max", "temperature_2m_min")))
    .select(
        F.col("subprefeitura_codigo"),
        F.to_date(F.col("z.time")).alias("data"),
        F.col("z.precipitation_sum").alias("chuva_mm"),
        F.col("z.temperature_2m_max").alias("temp_max_c"),
        F.col("z.temperature_2m_min").alias("temp_min_c"),
        "arquivo_origem", "carregado_em",
    )
)

cl_ok = aplicar_regras(cl, "clima", "subprefeitura_codigo", [
    ("CLIMA_R1", "Chuva ausente ou negativa", F.col("chuva_mm").isNull() | (F.col("chuva_mm") < 0)),
    ("CLIMA_R2", "Temperatura fora da faixa plausível para SP (-5 a 45 °C)",
     F.col("temp_max_c").isNull() | (F.col("temp_max_c") > 45) | (F.col("temp_min_c") < -5)),
    ("CLIMA_R3", "Subprefeitura ou data ausente", F.col("subprefeitura_codigo").isNull() | F.col("data").isNull()),
])
merge(ultimo_por_chave(cl_ok, ["subprefeitura_codigo", "data"]), "workspace.gs.prata_clima",
      ["subprefeitura_codigo", "data"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatos do app Caminho Livre: JSON aninhado aberto em colunas

# COMMAND ----------

esquema_usuario = "nome STRING, email STRING, telefone STRING"
esquema_local = "latitude DOUBLE, longitude DOUBLE, subprefeitura_codigo STRING"
esquema_det = "tipo STRING, confianca DOUBLE, gravidade STRING, passagem_possivel BOOLEAN"
TIPOS_VALIDOS = ["buraco", "desnivel", "obstaculo_fixo", "lixo_entulho", "veiculo_na_calcada",
                 "rampa_ausente", "piso_tatil_danificado", "calcada_estreita"]

rl = (
    spark.table("workspace.gs.bronze_relatos")
    .withColumn("u", F.from_json(como_json("bronze_relatos", "usuario"), esquema_usuario))
    .withColumn("l", F.from_json(como_json("bronze_relatos", "localizacao"), esquema_local))
    .withColumn("d", F.from_json(como_json("bronze_relatos", "deteccao"), esquema_det))
    .select(
        F.trim("id_relato").alias("id_relato"),
        F.expr("try_to_timestamp(enviado_em)").alias("enviado_em"),
        F.lower(F.trim("u.email")).alias("usuario_email"),
        F.initcap(F.trim("u.nome")).alias("usuario_nome"),
        F.col("u.telefone").alias("usuario_telefone"),
        F.col("l.latitude").alias("latitude"),
        F.col("l.longitude").alias("longitude"),
        F.upper(F.trim("l.subprefeitura_codigo")).alias("subprefeitura_codigo"),
        F.lower(F.trim("d.tipo")).alias("tipo_barreira"),
        F.col("d.confianca").alias("confianca"),
        F.coalesce(F.lower(F.trim("d.gravidade")), F.lit("media")).alias("gravidade"),
        F.coalesce(F.col("d.passagem_possivel"), F.lit(False)).alias("passagem_possivel"),
        F.col("versao_app"),
        "arquivo_origem", "carregado_em",
    )
    .withColumn("data_relato", F.to_date("enviado_em"))
)

rl_ok = aplicar_regras(rl, "relatos", "id_relato", [
    ("APP_R1", "Sem localização ou fora dos limites do município de SP",
     F.col("latitude").isNull() | F.col("longitude").isNull()
     | ~F.col("latitude").between(-24.01, -23.35) | ~F.col("longitude").between(-46.83, -46.36)),
    ("APP_R2", "Tipo de barreira fora do catálogo do app", ~F.col("tipo_barreira").isin(TIPOS_VALIDOS)),
    ("APP_R3", "Detecção com confiança abaixo de 0,5", F.coalesce(F.col("confianca"), F.lit(0.0)) < 0.5),
])
merge(ultimo_por_chave(rl_ok, ["id_relato"]), "workspace.gs.prata_relatos", ["id_relato"])

# COMMAND ----------

(spark.createDataFrame(qualidade, "execucao_id STRING, fonte STRING, regra STRING, descricao STRING, "
                                  "avaliados BIGINT, barrados BIGINT")
 .withColumn("executado_em", F.current_timestamp())
 .write.mode("append").saveAsTable("workspace.gs.controle_qualidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total barrado por regra nesta execução (print para o relatório)

# COMMAND ----------

display(spark.table("workspace.gs.controle_qualidade").filter(F.col("execucao_id") == EXECUCAO)
        .orderBy("fonte", "regra"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'prata_sp156' AS tabela, count(*) AS linhas FROM workspace.gs.prata_sp156
# MAGIC UNION ALL SELECT 'prata_clima', count(*) FROM workspace.gs.prata_clima
# MAGIC UNION ALL SELECT 'prata_relatos', count(*) FROM workspace.gs.prata_relatos;
