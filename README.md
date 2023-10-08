# Trabalho de Graduação
Repositório do Trabalho de Graduação: Família de Colunas em dados radar de tráfego aéreo


## Usage

### Instale o Docker:

https://docs.docker.com/desktop/install/windows-install/

### Construa o Container do Docker (Abra o VSCode no diretório) ou execute:
```
docker compose up -d
```
### Instale as dependências do Python:
```
poetry install
```
### Execute o setup do banco de dados.
```
poetry run python3 app/setup.py
```