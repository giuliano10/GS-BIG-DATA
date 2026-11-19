# Roteiro do relatório

1. **Integrantes:** nome completo e RM de todos.
2. **Produto em uma frase e perguntas do MVP:** copie do README.
3. **Diagrama do fluxo de dados:** fontes → Airflow → chegada/ → Volume → Bronze → Prata → Ouro → MVP.
4. **Tabela de fontes:** copie do README (origem, formato e frequência).
5. **Requisitos 01 a 09**, cada um com print e legenda:
   - 01: tabela de fontes e trecho do código de cada coleta
   - 02: DAG no código (`schedule="@daily"`) e a pasta `chegada/` com os arquivos datados
   - 03: visão Graph com as 3 coletas em paralelo, a tarefa `conferir_chegada` e a tela Admin › Variables (valor oculto)
   - 04: Volume com os arquivos do lote 1 e depois com os do lote 2
   - 05: tabela `controle_cargas` (1º lote, 2º lote e 2ª execução com 0 linhas novas)
   - 06: célula do MERGE e a tabela `controle_qualidade` com o total barrado por regra
   - 07: diagrama estrela e as 5 consultas
   - 08: `dim_usuario` com hash e máscara, a view `ouro_mvp_painel` e a tela do notebook 05
   - 09: README e a árvore do repositório no GitHub
6. **Prints do Airflow:** visões Graph e Grid com as execuções concluídas (verdes).
7. **Contagens da Bronze** antes e depois do 2º lote e da 2ª execução.
8. **As 5 consultas SQL**, cada uma com o resultado e 2–3 linhas de interpretação.
9. **Conclusão técnica** e link do repositório público.


