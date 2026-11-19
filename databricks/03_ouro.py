# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro: modelo dimensional e LGPD
# MAGIC **Estrela:** `fato_ocorrencia_calcada` com as dimensões `dim_data`, `dim_subprefeitura`, `dim_tipo_ocorrencia` e `dim_usuario`.
# MAGIC
# MAGIC Cada ocorrência é um problema de calçada registrado por um cidadão, pelo **SP156** (Prefeitura) ou pelo **app Caminho Livre**.
# MAGIC
# MAGIC **LGPD na Ouro:**
# MAGIC * o e-mail vira `sk_usuario` = **SHA-256(e-mail + sal secreto)**: não dá para voltar ao e-mail, mas o mesmo usuário continua sendo contado como um só;
# MAGIC * o nome e o e-mail ficam **mascarados** (`A*** T***`, `a***@exemplo.com`) e o telefone **não entra** na Ouro;
# MAGIC * a localização exata do relato é arredondada para **3 casas (~100 m)**, para não expor por onde a pessoa anda.
# MAGIC
# MAGIC O MVP só lê a view `ouro_mvp_painel`, que não tem nenhum dado pessoal (é o contrato com o produto).

# COMMAND ----------

# O sal NUNCA fica no código nem no repositório. Ordem de busca: secret scope "gs" e, se não existir, o campo do widget.
dbutils.widgets.text("sal_lgpd", "", "Sal LGPD (se não houver secret)")
try:
    SAL = dbutils.secrets.get("gs", "sal_lgpd")
except Exception:
    SAL = dbutils.widgets.get("sal_lgpd")
assert SAL, "Preencha o widget 'sal_lgpd' no topo do notebook (qualquer texto secreto, ex.: uma frase longa)."

from pyspark.sql import functions as F

# view temporária (só existe nesta sessão): relatos já com o e-mail transformado em hash; o telefone sai aqui
(spark.table("workspace.gs.prata_relatos")
 .withColumn("sk_usuario", F.sha2(F.concat(F.col("usuario_email"), F.lit(SAL)), 256))
 .drop("usuario_telefone")
 .createOrReplaceTempView("relatos_lgpd"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== dim_subprefeitura =====
# MAGIC CREATE OR REPLACE TABLE workspace.gs.dim_subprefeitura AS
# MAGIC SELECT row_number() OVER (ORDER BY codigo) AS sk_subprefeitura,
# MAGIC        codigo AS subprefeitura_codigo, subprefeitura, zona, latitude, longitude
# MAGIC FROM workspace.gs.ref_subprefeitura;
# MAGIC
# MAGIC -- ===== dim_tipo_ocorrencia (serviço do SP156 ou tipo detectado pelo app) =====
# MAGIC CREATE OR REPLACE TABLE workspace.gs.dim_tipo_ocorrencia AS
# MAGIC WITH tipos AS (
# MAGIC   SELECT DISTINCT 'SP156' AS origem, servico AS tipo FROM workspace.gs.prata_sp156
# MAGIC   UNION
# MAGIC   SELECT DISTINCT 'APP', tipo_barreira FROM workspace.gs.prata_relatos
# MAGIC )
# MAGIC SELECT row_number() OVER (ORDER BY origem, tipo) AS sk_tipo, origem, tipo,
# MAGIC        CASE
# MAGIC          WHEN lower(tipo) RLIKE 'buraco|desnivel|piso|reforma|reparo|irregular|danific|manuten' THEN 'Pavimento danificado'
# MAGIC          WHEN lower(tipo) RLIKE 'obstaculo|lixo|entulho|veiculo|ocupa|comercio|estreita|fiac|fiaç' THEN 'Calçada obstruída'
# MAGIC          WHEN lower(tipo) RLIKE 'rampa|acessib|tatil|constru' THEN 'Falta de acessibilidade'
# MAGIC          ELSE 'Outros problemas de calçada'
# MAGIC        END AS categoria
# MAGIC FROM tipos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== dim_usuario (LGPD: hash + máscara, sem telefone) =====
# MAGIC CREATE OR REPLACE TABLE workspace.gs.dim_usuario AS
# MAGIC WITH u AS (
# MAGIC   SELECT sk_usuario, max(usuario_email) AS usuario_email, max_by(usuario_nome, enviado_em) AS usuario_nome,
# MAGIC          min(data_relato) AS primeiro_relato, count(*) AS total_relatos
# MAGIC   FROM relatos_lgpd
# MAGIC   WHERE usuario_email IS NOT NULL
# MAGIC   GROUP BY sk_usuario
# MAGIC )
# MAGIC SELECT sk_usuario,
# MAGIC        concat(left(usuario_email, 1), '***@', split_part(usuario_email, '@', 2))     AS email_mascarado,
# MAGIC        concat_ws(' ', transform(split(usuario_nome, ' '), p -> concat(left(p, 1), '***'))) AS nome_mascarado,
# MAGIC        primeiro_relato, total_relatos
# MAGIC FROM u;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== fato_ocorrencia_calcada (grão: 1 linha por ocorrência registrada) =====
# MAGIC CREATE OR REPLACE TABLE workspace.gs.fato_ocorrencia_calcada AS
# MAGIC WITH sp AS (
# MAGIC   SELECT concat('SP156-', left(id_solicitacao, 16)) AS id_ocorrencia, 'SP156' AS origem,
# MAGIC          data_abertura AS data_ocorrencia, subprefeitura_codigo, servico AS tipo,
# MAGIC          CAST(NULL AS STRING) AS sk_usuario, canal, status, resolvida, dias_atendimento,
# MAGIC          CAST(NULL AS STRING) AS gravidade, CAST(NULL AS BOOLEAN) AS passagem_possivel,
# MAGIC          CAST(NULL AS DOUBLE) AS confianca, round(latitude, 3) AS lat_aprox, round(longitude, 3) AS lon_aprox
# MAGIC   FROM workspace.gs.prata_sp156
# MAGIC ),
# MAGIC app AS (
# MAGIC   SELECT concat('APP-', id_relato), 'APP', data_relato, subprefeitura_codigo, tipo_barreira,
# MAGIC          sk_usuario, 'App Caminho Livre', 'REPORTADA', false, CAST(NULL AS INT),
# MAGIC          gravidade, passagem_possivel, confianca, round(latitude, 3), round(longitude, 3)
# MAGIC   FROM relatos_lgpd
# MAGIC ),
# MAGIC tudo AS (SELECT * FROM sp UNION ALL SELECT * FROM app)
# MAGIC SELECT t.id_ocorrencia,
# MAGIC        CAST(date_format(t.data_ocorrencia, 'yyyyMMdd') AS INT) AS sk_data,
# MAGIC        s.sk_subprefeitura, ti.sk_tipo, t.sk_usuario,
# MAGIC        t.origem, t.canal, t.status, t.resolvida, t.dias_atendimento,
# MAGIC        t.gravidade, t.passagem_possivel, t.confianca, t.lat_aprox, t.lon_aprox,
# MAGIC        c.chuva_mm AS chuva_mm_no_dia,
# MAGIC        1 AS qtd_ocorrencias
# MAGIC FROM tudo t
# MAGIC JOIN workspace.gs.dim_subprefeitura s ON s.subprefeitura_codigo = t.subprefeitura_codigo
# MAGIC JOIN workspace.gs.dim_tipo_ocorrencia ti ON ti.origem = t.origem AND ti.tipo = t.tipo
# MAGIC LEFT JOIN workspace.gs.prata_clima c
# MAGIC   ON c.subprefeitura_codigo = t.subprefeitura_codigo AND c.data = t.data_ocorrencia;
# MAGIC
# MAGIC -- ===== dim_data (calendário cobrindo todas as datas da fato) =====
# MAGIC CREATE OR REPLACE TABLE workspace.gs.dim_data AS
# MAGIC WITH limites AS (
# MAGIC   SELECT to_date(CAST(min(sk_data) AS STRING), 'yyyyMMdd') AS ini,
# MAGIC          to_date(CAST(max(sk_data) AS STRING), 'yyyyMMdd') AS fim
# MAGIC   FROM workspace.gs.fato_ocorrencia_calcada
# MAGIC )
# MAGIC SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS sk_data, d AS data,
# MAGIC        year(d) AS ano, month(d) AS mes, date_format(d, 'yyyy-MM') AS ano_mes,
# MAGIC        weekofyear(d) AS semana_ano, dayofweek(d) AS dia_semana,
# MAGIC        CASE dayofweek(d) WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Segunda' WHEN 3 THEN 'Terça' WHEN 4 THEN 'Quarta'
# MAGIC             WHEN 5 THEN 'Quinta' WHEN 6 THEN 'Sexta' ELSE 'Sábado' END AS nome_dia,
# MAGIC        dayofweek(d) IN (1, 7) AS fim_de_semana
# MAGIC FROM limites LATERAL VIEW explode(sequence(ini, fim, INTERVAL 1 DAY)) cal AS d;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== Contrato com o MVP: o que a tela do cidadão consome (sem nenhum dado pessoal) =====
# MAGIC CREATE OR REPLACE VIEW workspace.gs.ouro_mvp_painel
# MAGIC COMMENT 'Contrato com o MVP Caminho Livre: situação das calçadas por subprefeitura. Sem dados pessoais.'
# MAGIC AS
# MAGIC WITH ultimo_dia AS (SELECT max(d.data) AS hoje FROM workspace.gs.fato_ocorrencia_calcada f
# MAGIC                     JOIN workspace.gs.dim_data d USING (sk_data) WHERE f.origem = 'APP')
# MAGIC SELECT s.subprefeitura, s.zona,
# MAGIC        count_if(f.origem = 'SP156')                                       AS solicitacoes_sp156,
# MAGIC        count_if(f.origem = 'SP156' AND NOT f.resolvida)                   AS solicitacoes_em_aberto,
# MAGIC        round(avg(f.dias_atendimento), 1)                                  AS espera_media_dias,
# MAGIC        count_if(f.origem = 'APP' AND d.data > u.hoje - INTERVAL 7 DAYS)   AS barreiras_app_7_dias,
# MAGIC        count_if(f.origem = 'APP' AND f.gravidade = 'alta')                AS barreiras_graves,
# MAGIC        count_if(f.origem = 'APP' AND NOT f.passagem_possivel)             AS barreiras_sem_passagem,
# MAGIC        round(max(c.chuva_7d), 1)                                          AS chuva_7_dias_mm
# MAGIC FROM workspace.gs.dim_subprefeitura s
# MAGIC CROSS JOIN ultimo_dia u
# MAGIC LEFT JOIN workspace.gs.fato_ocorrencia_calcada f ON f.sk_subprefeitura = s.sk_subprefeitura
# MAGIC LEFT JOIN workspace.gs.dim_data d ON d.sk_data = f.sk_data
# MAGIC LEFT JOIN (SELECT subprefeitura_codigo, sum(chuva_mm) AS chuva_7d FROM workspace.gs.prata_clima
# MAGIC            WHERE data > (SELECT max(data) FROM workspace.gs.prata_clima) - INTERVAL 7 DAYS
# MAGIC            GROUP BY subprefeitura_codigo) c ON c.subprefeitura_codigo = s.subprefeitura_codigo
# MAGIC GROUP BY s.subprefeitura, s.zona;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência da Ouro (prints para o relatório)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fato_ocorrencia_calcada' AS tabela, count(*) AS linhas FROM workspace.gs.fato_ocorrencia_calcada
# MAGIC UNION ALL SELECT 'dim_data', count(*) FROM workspace.gs.dim_data
# MAGIC UNION ALL SELECT 'dim_subprefeitura', count(*) FROM workspace.gs.dim_subprefeitura
# MAGIC UNION ALL SELECT 'dim_tipo_ocorrencia', count(*) FROM workspace.gs.dim_tipo_ocorrencia
# MAGIC UNION ALL SELECT 'dim_usuario', count(*) FROM workspace.gs.dim_usuario;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- LGPD: como o usuário aparece na Ouro (comparar com a Prata, que tem o e-mail aberto e fica restrita)
# MAGIC SELECT * FROM workspace.gs.dim_usuario LIMIT 5;
