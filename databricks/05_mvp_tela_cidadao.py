# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · MVP Caminho Livre: tela do cidadão
# MAGIC Esta tela consome **somente** a view da Ouro `workspace.gs.ouro_mvp_painel`, que é o contrato com o produto e não tem dados pessoais.
# MAGIC Também lê a `fato_ocorrencia_calcada` agregada, sem identificar ninguém.
# MAGIC
# MAGIC Escolha a sua subprefeitura no campo do topo e rode a célula de novo.

# COMMAND ----------

subs = [r.subprefeitura for r in spark.sql(
    "SELECT subprefeitura FROM workspace.gs.ouro_mvp_painel ORDER BY subprefeitura").collect()]
dbutils.widgets.dropdown("subprefeitura", "Sé" if "Sé" in subs else subs[0], subs, "Sua subprefeitura")

# COMMAND ----------

import html

escolha = dbutils.widgets.get("subprefeitura")
p = spark.sql("SELECT * FROM workspace.gs.ouro_mvp_painel WHERE subprefeitura = :s", args={"s": escolha}).first()
media = spark.sql("SELECT avg(espera_media_dias) AS m FROM workspace.gs.ouro_mvp_painel").first().m or 0

top = spark.sql("""
    SELECT t.categoria, count(*) AS qtd
    FROM workspace.gs.fato_ocorrencia_calcada f
    JOIN workspace.gs.dim_subprefeitura s ON s.sk_subprefeitura = f.sk_subprefeitura
    JOIN workspace.gs.dim_tipo_ocorrencia t ON t.sk_tipo = f.sk_tipo
    WHERE s.subprefeitura = :s
    GROUP BY t.categoria ORDER BY qtd DESC LIMIT 4
""", args={"s": escolha}).collect()

espera = p.espera_media_dias or 0
if p.barreiras_sem_passagem > 0 or espera > media * 1.2:
    alerta, cor = "Atenção: trechos com barreiras que bloqueiam a passagem", "#b3261e"
elif p.barreiras_app_7_dias > 0:
    alerta, cor = "Há barreiras reportadas recentemente", "#9a6700"
else:
    alerta, cor = "Sem barreiras reportadas na última semana", "#1a7f37"

itens = "".join(f"<li>{html.escape(r.categoria)}: <b>{r.qtd}</b></li>" for r in top) or "<li>Sem registros</li>"


def cartao(valor, rotulo):
    return (f"<div style='flex:1;min-width:150px;background:#f4f6f8;border-radius:12px;padding:14px'>"
            f"<div style='font-size:28px;font-weight:700'>{valor}</div>"
            f"<div style='font-size:13px;color:#444'>{rotulo}</div></div>")


displayHTML(f"""
<div style="font-family:Segoe UI,Arial,sans-serif;max-width:760px">
  <div style="font-size:13px;color:#666">Caminho Livre · calçadas de São Paulo</div>
  <h2 style="margin:4px 0 2px">{html.escape(p.subprefeitura)} <span style="font-size:14px;color:#666">· Zona {html.escape(p.zona)}</span></h2>
  <div style="background:{cor};color:white;border-radius:8px;padding:10px 14px;margin:10px 0;font-weight:600">{alerta}</div>
  <div style="display:flex;gap:10px;flex-wrap:wrap">
    {cartao(p.barreiras_app_7_dias, "barreiras reportadas no app (7 dias)")}
    {cartao(p.barreiras_sem_passagem, "barreiras sem passagem possível")}
    {cartao(p.solicitacoes_em_aberto, "pedidos de conserto em aberto no SP156")}
    {cartao(f"{espera:.0f} dias", f"espera média por conserto (cidade: {media:.0f} dias)")}
  </div>
  <h3 style="margin:18px 0 6px">Problemas mais comuns na sua região</h3>
  <ul style="margin:0">{itens}</ul>
  <p style="font-size:13px;color:#444;margin-top:14px">Chuva nos últimos 7 dias: <b>{p.chuva_7_dias_mm or 0} mm</b>.
  Com chuva, buracos e desníveis ficam mais perigosos para cadeirantes e pessoas com deficiência visual.</p>
  <p style="font-size:12px;color:#888">Fonte: SP156 (Prefeitura de SP, dados abertos), Open-Meteo e relatos do app Caminho Livre.
  Nenhum dado pessoal é exibido.</p>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visão da cidade (mesma view do contrato)

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.gs.ouro_mvp_painel ORDER BY barreiras_sem_passagem DESC, espera_media_dias DESC"))
