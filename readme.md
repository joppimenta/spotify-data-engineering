# Spotify Data Engineering Project

## Descrição
Este projeto tem como objetivo extrair dados da API do Spotify via Python de dados referentes às músicas escutadas por um usuário, processá-los via DBT e armazená-los no **Google BigQuery**.  
O pipeline é estruturado no formato **ELT**: os dados são extraídos da API, carregados em uma landing table no BigQuery e transformados via DBT. Esse processo é orquestrado via GitHub Actions, que também agenda uma execução diária dessa tarefa.

## Funcionalidades
- Autenticação e refresh de token na API do Spotify.
- Extração de músicas recentemente tocadas pelo usuário.
- Carregamento de dados no BigQuery.
- Registro da data/hora do carregamento.
- Consulta incremental, buscando apenas músicas tocadas após a última registrada.
- Projeto DBT que contém os SQLs que transformam os dados da landing table para o Datalake e Datawarehouse, respectivamente.
- Workflow de orquestração para agendar a execução do meu projeto via Github Actions

## Estrutura do Projeto

```
spotify-project/ # Pasta que contém os pythons usados para extrair os dados da APi do Spotify e enviar ao BigQuery
├── spotify-extract-data/
│   ├── main.py
│   ├── spotify_api.py
├── spotify-dbt/ # Pasta onde o projeto do dbt está armazenado
│   └── spotify_project/ # projeto do dbt
│       └── models/ # Local onde está armazenado as transformações SQL
│           └── staging/ # SQLs que geram o datalake
│           └── datamart/ # SQLs que geram o datawarehouse
├── .github/ # Pasta que armazena o meu workflow que agenda a execução do python e as transformações do DBT via Github Actions
    └── workflows/ # Armazenador dos arquivos .yml (workflows)
├── requirements.txt
└── README.md
```

# Tecnologias Utilizadas

- Python

- Pandas / Pandas-GBQ

- Google BigQuery

- DBT

- GitHub Actions
