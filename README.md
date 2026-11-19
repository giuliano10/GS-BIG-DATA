## Como reproduzir

### 1. Airflow (coleta)
Pré-requisito: Docker Desktop.

```bash
git clone <este repositório> && cd gs-big-data
mkdir chegada logs
docker compose up -d          # aguarde ~1 min e abra http://localhost:8080
```

Em **Admin › Variables**, crie:

| Variable | Obrigatória | Valor |
|---|---|---|
| `relatos_api_token` | sim | qualquer token (simula a chave da API do app) |
| `webhook_notificacao` | não | URL de webhook (Teams/Discord) para receber as notificações |
| `sp156_csv_url` | não | URL direta de um CSV do SP156, caso a API CKAN esteja fora do ar |

Ative o DAG `gs_coleta_calcadas`. Com `catchup=True` ele executa um dia por vez desde 10/11/2026, e cada execução grava
`chegada/sp156/sp156_AAAA-MM-DD.csv`, `chegada/clima/clima_AAAA-MM-DD.json` e `chegada/relatos/relatos_AAAA-MM-DD.json`.
As notificações ficam em `chegada/_notificacoes/notificacoes.log`.

> Se a rede bloquear o download do SP156, baixe o CSV mais recente pelo navegador em dados.prefeitura.sp.gov.br › "Dados do SP156" e salve como `chegada/_cache/sp156/manual.csv`.

### 2. Databricks (lakehouse)
1. Em **Workspace › Import**, importe os arquivos da pasta `databricks/`.
2. Rode `00_setup` (cria o schema `workspace.gs`, o Volume `chegada` e as subpastas).
3. **Lote 1:** em **Catalog › workspace › gs › chegada**, envie para as pastas `sp156/`, `clima/` e `relatos/` os arquivos dos primeiros dias de coleta. Depois rode `01_bronze` e `02_prata`.
4. **Lote 2:** envie os arquivos dos dias seguintes e rode `01_bronze` de novo (só os novos entram). Rode `01_bronze` mais uma vez sem arquivos novos para mostrar que entram 0 linhas. Depois rode `02_prata` e `03_ouro` (preencha o widget `sal_lgpd`).
5. Rode `04_consultas_ouro` e `05_mvp_tela_cidadao`.

## LGPD
* Os dados pessoais do app (nome, e-mail, telefone) são **fictícios** (`@exemplo.com`) e só existem na Prata.
* Na Ouro, o e-mail vira `sk_usuario = SHA-256(e-mail + sal)`, o nome e o e-mail aparecem mascarados, o telefone é descartado e as coordenadas são arredondadas.
* O sal fica num secret scope do Databricks ou é digitado no widget. **Nunca vai para o repositório.**
* O MVP consome somente `ouro_mvp_painel`, que não tem dado pessoal.
* O repositório não tem chaves nem senhas: as configurações ficam em Airflow Variables, e `chegada/` e `logs/` estão no `.gitignore`.
