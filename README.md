## Ficha Técnica do MVP

| Campo | Resposta |
|---|---|
| **Nome do produto** | Caminho Livre: tecnologia para uma cidade acessível |
| **Cidadão e território** | Pessoas com deficiência, idosos e pessoas com mobilidade reduzida, na cidade de São Paulo |
| **Problema, em uma frase** | Calçadas com buracos, desníveis, obstáculos e obras dificultam a circulação de quem tem mobilidade reduzida, e os relatos chegam dispersos, sem um processo ágil para conferir, priorizar e encaminhar |
| **Proposta de valor** | O cidadão registra o obstáculo com foto, rua e ponto de referência. A plataforma identifica a barreira, prioriza e encaminha as ocorrências aos órgãos públicos, alimenta um painel de dados e um assistente responde dúvidas sobre as normas citando o artigo |
| **Arquitetura: componentes** | Coleta e dados: Airflow + Databricks (Bronze, Prata, Ouro). RPA: robô Python a cada 1 min + servidor com formulário, painel e API REST + SQLite. Triagem visual: Python, OpenCV, Tkinter. Assistente de normas: FastAPI, busca híbrida e LLM com LoRA |
| **Fontes de dados** | SP156 (Portal de Dados Abertos da Prefeitura de SP), Open-Meteo, relatos do app e CSVs da prefeitura e de ONGs (fictícios nos testes), 7 normas federais e municipais, imagens anotadas no Roboflow |
| **Modelos de IA utilizados** | RF-DETR (Roboflow), YOLOv8n, Qwen2.5-1.5B-Instruct + LoRA, multilingual-e5-small |
| **URL do MVP e repositório** | MVP executável localmente (formulário em localhost:8000, painel em /painel). Repositórios: [Generative AI](https://github.com/giuliano10/GS-GENERATIVE-AI) · [Applied Vision](https://github.com/giuliano10/GS-AMPPLIED-VISION) · RPA: [https://github.com/giuliano10/GS-RPA] · Big Data: [https://github.com/giuliano10/GS-BIG-DATA] |
| **Indicadores de impacto** | Assistente: 75% de acurácia, 13,3% de alucinação e 86,7% de recusa correta (60 perguntas de teste). RPA: 30 testes unitários e 48 verificações ponta a ponta. Dados pessoais apagados após 90 dias |
| **ODS conectados** | ODS 11 (cidades e comunidades sustentáveis) e ODS 10 (redução das desigualdades) |



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
