-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · As 5 consultas SQL que respondem às perguntas do MVP
-- MAGIC | # | Pergunta do cidadão / gestor | Fonte |
-- MAGIC |---|---|---|
-- MAGIC | 1 | Onde estão as calçadas com mais problemas? | SP156 + app |
-- MAGIC | 2 | **(IMPACTO)** Quanto tempo o cidadão espera para ter a calçada consertada, e onde espera mais? | SP156 |
-- MAGIC | 3 | Quais problemas de calçada a população mais pede para a Prefeitura resolver, e quais ela resolve? | SP156 |
-- MAGIC | 4 | Quais barreiras o app mais detecta e quais impedem a passagem? | app + clima |
-- MAGIC | 5 | As reclamações de calçada estão aumentando? | SP156 |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Ranking das subprefeituras com mais ocorrências de calçada

-- COMMAND ----------

SELECT s.subprefeitura, s.zona,
       sum(f.qtd_ocorrencias)                                        AS ocorrencias,
       count_if(f.origem = 'SP156')                                  AS via_sp156,
       count_if(f.origem = 'APP')                                    AS via_app,
       round(100 * sum(f.qtd_ocorrencias) / sum(sum(f.qtd_ocorrencias)) OVER (), 1) AS pct_da_cidade
FROM workspace.gs.fato_ocorrencia_calcada f
JOIN workspace.gs.dim_subprefeitura s ON s.sk_subprefeitura = f.sk_subprefeitura
GROUP BY s.subprefeitura, s.zona
ORDER BY ocorrencias DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. (IMPACTO) Tempo de espera do cidadão por subprefeitura
-- MAGIC Mostra a espera média e o P90 (9 em cada 10 pedidos são atendidos até esse prazo), quanto ainda está em aberto
-- MAGIC e quantas vezes a espera passa da média da cidade. Esses números indicam onde a calçada ruim fica mais tempo sem conserto.

-- COMMAND ----------

WITH base AS (
  SELECT s.subprefeitura,
         count(*)                                   AS solicitacoes,
         count_if(NOT f.resolvida)                  AS em_aberto,
         round(avg(f.dias_atendimento), 1)          AS espera_media_dias,
         percentile_approx(f.dias_atendimento, 0.9) AS espera_p90_dias
  FROM workspace.gs.fato_ocorrencia_calcada f
  JOIN workspace.gs.dim_subprefeitura s ON s.sk_subprefeitura = f.sk_subprefeitura
  WHERE f.origem = 'SP156'
  GROUP BY s.subprefeitura
)
SELECT subprefeitura, solicitacoes, em_aberto,
       round(100 * em_aberto / solicitacoes, 1)                    AS pct_em_aberto,
       espera_media_dias, espera_p90_dias,
       round(espera_media_dias / avg(espera_media_dias) OVER (), 2) AS vezes_a_media_da_cidade
FROM base
ORDER BY espera_media_dias DESC NULLS LAST
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. O que a população mais pede ao SP156 sobre calçadas, e quanto é resolvido

-- COMMAND ----------

SELECT t.tipo AS servico, t.categoria,
       count(*)                                               AS solicitacoes,
       round(100 * avg(CAST(f.resolvida AS INT)), 1)          AS pct_resolvidas,
       round(avg(f.dias_atendimento), 1)                      AS espera_media_dias
FROM workspace.gs.fato_ocorrencia_calcada f
JOIN workspace.gs.dim_tipo_ocorrencia t ON t.sk_tipo = f.sk_tipo
WHERE f.origem = 'SP156'
GROUP BY t.tipo, t.categoria
ORDER BY solicitacoes DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Barreiras detectadas pelo app: gravidade, bloqueio da passagem e chuva

-- COMMAND ----------

SELECT t.tipo AS barreira, t.categoria,
       count(*)                                                                  AS relatos,
       count(DISTINCT f.sk_usuario)                                              AS cidadaos_que_reportaram,
       round(100 * avg(CASE WHEN f.gravidade = 'alta' THEN 1 ELSE 0 END), 1)     AS pct_graves,
       round(100 * avg(CASE WHEN NOT f.passagem_possivel THEN 1 ELSE 0 END), 1)  AS pct_sem_passagem,
       round(100 * avg(CASE WHEN f.chuva_mm_no_dia >= 1 THEN 1 ELSE 0 END), 1)   AS pct_em_dia_de_chuva
FROM workspace.gs.fato_ocorrencia_calcada f
JOIN workspace.gs.dim_tipo_ocorrencia t ON t.sk_tipo = f.sk_tipo
WHERE f.origem = 'APP'
GROUP BY t.tipo, t.categoria
ORDER BY relatos DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Evolução mensal das solicitações de calçada no SP156

-- COMMAND ----------

SELECT d.ano_mes,
       count(*)                                                AS solicitacoes,
       count(*) - lag(count(*)) OVER (ORDER BY d.ano_mes)      AS variacao_mes_anterior,
       count_if(f.resolvida)                                   AS resolvidas,
       round(avg(f.dias_atendimento), 1)                       AS espera_media_dias
FROM workspace.gs.fato_ocorrencia_calcada f
JOIN workspace.gs.dim_data d ON d.sk_data = f.sk_data
WHERE f.origem = 'SP156'
GROUP BY d.ano_mes
ORDER BY d.ano_mes;
