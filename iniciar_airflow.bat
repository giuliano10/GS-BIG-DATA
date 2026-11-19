@echo off
REM Sobe o Airflow 3 do projeto, cria as Variables e ativa o DAG. Basta dar dois cliques.
cd /d "%~dp0"
if not exist chegada mkdir chegada
if not exist logs mkdir logs

echo [1/5] Verificando o Docker...
docker info >nul 2>&1
if not errorlevel 1 goto dockerok
echo Abrindo o Docker Desktop, aguarde...
start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"
:esperadocker
timeout /t 5 /nobreak >nul
docker info >nul 2>&1
if errorlevel 1 goto esperadocker
:dockerok

echo [2/5] Subindo Airflow + Postgres (a primeira vez baixa as imagens, pode levar alguns minutos)...
docker compose up -d
if errorlevel 1 goto erro

echo [3/5] Aguardando o Airflow iniciar...
set /a tentativas=0
:esperaairflow
set /a tentativas+=1
if %tentativas% gtr 60 goto erro
timeout /t 10 /nobreak >nul
docker compose exec -T airflow airflow dags list >nul 2>&1
if errorlevel 1 goto esperaairflow

echo [4/5] Criando as Variables (token gerado agora, nao fica salvo em arquivo)...
docker compose exec -T airflow airflow variables get relatos_api_token >nul 2>&1
if errorlevel 1 docker compose exec -T airflow airflow variables set relatos_api_token "tk-%RANDOM%%RANDOM%%RANDOM%"

echo [5/5] Ativando o DAG gs_coleta_calcadas...
docker compose exec -T airflow airflow dags unpause gs_coleta_calcadas

echo.
echo Pronto! Abra http://localhost:8080 no navegador.
start "" http://localhost:8080
pause
exit /b 0

:erro
echo Algo deu errado. Copie a mensagem acima e mande para o Claude.
pause
exit /b 1
