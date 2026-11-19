# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do lakehouse (rodar uma vez)
# MAGIC Cria o schema `workspace.gs`, o Volume `chegada` (pasta de pouso), as subpastas de cada fonte,
# MAGIC as tabelas de controle e a tabela de referência das 32 subprefeituras de São Paulo.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gs COMMENT 'GS 2026.2 · Caminho Livre · calçadas de SP'")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.gs.chegada COMMENT 'Pasta de pouso: arquivos vindos do Airflow'")

BASE = "/Volumes/workspace/gs/chegada"
for pasta in ["sp156", "clima", "relatos", "_controle"]:
    dbutils.fs.mkdirs(f"{BASE}/{pasta}")
display(dbutils.fs.ls(BASE))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- contagens da Bronze antes/depois de cada carga (requisito 05)
# MAGIC CREATE TABLE IF NOT EXISTS workspace.gs.controle_cargas (
# MAGIC   execucao_id STRING, tabela STRING, linhas_antes BIGINT, linhas_depois BIGINT,
# MAGIC   linhas_novas BIGINT, arquivos_total BIGINT, executado_em TIMESTAMP);
# MAGIC
# MAGIC -- total barrado por regra de qualidade (requisito 06)
# MAGIC CREATE TABLE IF NOT EXISTS workspace.gs.controle_qualidade (
# MAGIC   execucao_id STRING, fonte STRING, regra STRING, descricao STRING,
# MAGIC   avaliados BIGINT, barrados BIGINT, executado_em TIMESTAMP);
# MAGIC
# MAGIC -- registros barrados, para auditoria
# MAGIC CREATE TABLE IF NOT EXISTS workspace.gs.prata_quarentena (
# MAGIC   execucao_id STRING, fonte STRING, regra STRING, chave STRING, registro STRING, executado_em TIMESTAMP);

# COMMAND ----------

REF_CSV = """codigo,subprefeitura,zona,latitude,longitude
AF,Aricanduva/Formosa/Carrão,Leste,-23.565,-46.513
BT,Butantã,Oeste,-23.571,-46.708
CL,Campo Limpo,Sul,-23.632,-46.759
CS,Capela do Socorro,Sul,-23.721,-46.700
CV,Casa Verde/Cachoeirinha,Norte,-23.497,-46.660
AD,Cidade Ademar,Sul,-23.668,-46.646
CT,Cidade Tiradentes,Leste,-23.582,-46.400
EM,Ermelino Matarazzo,Leste,-23.502,-46.481
FO,Freguesia/Brasilândia,Norte,-23.472,-46.697
G,Guaianases,Leste,-23.541,-46.410
IP,Ipiranga,Sul,-23.600,-46.610
IT,Itaim Paulista,Leste,-23.500,-46.400
IQ,Itaquera,Leste,-23.540,-46.455
JA,Jabaquara,Sul,-23.646,-46.641
JT,Jaçanã/Tremembé,Norte,-23.452,-46.600
LA,Lapa,Oeste,-23.522,-46.704
MB,M'Boi Mirim,Sul,-23.686,-46.770
MO,Mooca,Leste,-23.555,-46.595
PA,Parelheiros,Sul,-23.830,-46.730
PE,Penha,Leste,-23.528,-46.540
PR,Perus/Anhanguera,Norte,-23.405,-46.750
PI,Pinheiros,Oeste,-23.567,-46.692
PJ,Pirituba/Jaraguá,Norte,-23.480,-46.735
ST,Santana/Tucuruvi,Norte,-23.490,-46.625
SA,Santo Amaro,Sul,-23.650,-46.705
SM,São Mateus,Leste,-23.605,-46.475
MP,São Miguel Paulista,Leste,-23.497,-46.445
SB,Sapopemba,Leste,-23.598,-46.507
SE,Sé,Centro,-23.548,-46.636
MG,Vila Maria/Vila Guilherme,Norte,-23.512,-46.586
VM,Vila Mariana,Sul,-23.589,-46.634
VP,Vila Prudente,Leste,-23.585,-46.565"""
linhas = [l.split(",") for l in REF_CSV.strip().splitlines()]
df_ref = spark.createDataFrame(
    [(c, s, z, float(la), float(lo)) for c, s, z, la, lo in linhas[1:]],
    "codigo STRING, subprefeitura STRING, zona STRING, latitude DOUBLE, longitude DOUBLE",
)
df_ref.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gs.ref_subprefeitura")
display(spark.table("workspace.gs.ref_subprefeitura"))
