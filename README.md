# Data Pipeline

Pipeline de validacao, transformacao e padronizacao de arquivos geoespaciais
em lote, orientado pela planilha de ingestao `input/st_Ingest_parameter.xlsx`.

O projeto processa bases `.shp` e `.gpkg`, valida schemas e dominios,
normaliza atributos, repara geometrias, calcula metricas espaciais e grava
saidas finais em GeoPackage.

## Objetivo

- Ler uma fila de processamento a partir da aba `datas` da planilha de ingestao.
- Processar arquivos geoespaciais por perfil de regras em `rules/`.
- Validar estrutura tabular contra a aba `dictionaries`.
- Preservar atributos originais com prefixo `sdb_*`.
- Gerar campos tratados e padronizados com prefixo `acm_*`.
- Produzir saidas em `output/<theme_folder>/` com logs e relatorios auxiliares.

## Entradas Suportadas

- Arquivos `.shp`.
- Arquivos `.gpkg`.
- Pastas contendo `.shp` e `.gpkg`, inclusive em subpastas.

Arquivos `.zip` nao sao processados diretamente.

Uma linha entra na fila quando:

- `status = Waiting Update`;
- `path_shapefile_temp` aponta para um arquivo ou pasta suportada;
- `theme_folder` encontra um perfil correspondente em `rules/`.

## Estrutura

```text
data-pipeline/
|-- main.py
|-- settings.py
|-- input/
|   `-- st_Ingest_parameter.xlsx
|-- output/
|-- core/
|   |-- ingest/
|   |-- processing/
|   |-- queue/
|   |-- rules/
|   |-- spatial/
|   |-- validation/
|   `-- output/
|-- projects/
|   |-- configs.py
|   |-- registry.py
|   `-- functions/
|-- rules/
|   |-- _template/
|   |-- app_car/
|   |-- reserva_legal_car/
|   |-- estado/
|   `-- autorizacao_para_supressao_vegetal/
|-- readme/
`-- tests/
```

Componentes principais:

- `main.py`: ponto de entrada da fila automatica.
- `settings.py`: configuracoes centrais do pipeline.
- `core/`: motor de ingestao, validacao, processamento, regras e escrita.
- `projects/`: configuracoes e funcoes especificas por projeto.
- `rules/`: perfis JSON modulares por tema e UF.
- `input/`: planilha de ingestao.
- `output/`: resultados gerados.

## Requisitos

- Python 3.14 ou superior.
- Dependencias declaradas em `pyproject.toml`.
- Ambiente recomendado com `uv`.

Instalacao:

```powershell
uv sync
```

Alternativa com `pip`:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -3.14 -m pip install --upgrade pip
py -3.14 -m pip install -e .
```

## Como Usar

1. Atualize `input/st_Ingest_parameter.xlsx`.
2. Na aba `datas`, defina `status = Waiting Update` para as linhas que devem ser processadas.
3. Preencha `path_shapefile_temp`, `theme_folder` e `theme`.
4. Confira se existe um perfil correspondente em `rules/`.
5. Execute o pipeline:

```powershell
uv run python main.py
```

Ou, usando o Python instalado diretamente:

```powershell
py -3.14 main.py
```

As saidas ficam em:

```text
output/<theme_folder>/
```

## Regras Modulares

Cada perfil em `rules/` deve conter cinco arquivos:

```text
rules/<projeto>/<perfil>/
|-- profile.json
|-- input_schema.json
|-- domains.json
|-- relations.json
`-- pipeline.json
```

Exemplo:

```text
theme_folder = app_car_es
perfil esperado = rules/app_car/app_car_es/
```

Associacoes principais:

- `app_car_*` usa `rules/app_car/`.
- `rl_car_*` usa `rules/reserva_legal_car/`.
- `estado` usa `rules/estado/`.
- `auth_supn` usa `rules/autorizacao_para_supressao_vegetal/`.

Use `rules/_template/` como base para novos perfis. O formato completo esta em
`readme/rules.md`.

## Convencoes de Colunas

- Colunas originais sao preservadas como `sdb_*`.
- Colunas tratadas, normalizadas ou derivadas sao gravadas como `acm_*`.
- Funcoes genericas do `core` nao devem sobrescrever valores `sdb_*`.
- Marcacoes tecnicas internas nao devem aparecer no GeoPackage final.

## Geometria

O pipeline:

- achata geometrias para 2D;
- repara geometrias invalidas quando possivel;
- valida geometrias OGC quando habilitado;
- calcula area, perimetro, longitude e latitude;
- usa `EPSG:4326` para saida e `EPSG:5880` para metricas;
- aplica validacao regional de bounding box para bases `app_car` e
  `reserva_legal_car`.

## Saidas

O arquivo principal de saida e sempre `.gpkg`.

Tambem podem ser gerados:

- log contextual `.txt`;
- relatorio de inconsistencias de dominio;
- relatorio de duplicados por atributos;
- relatorio de duplicados geometricos;
- relatorio de geometrias invalidas OGC;
- consolidado por grupo, quando `ENABLE_GROUP_CONSOLIDATION = True`.

## Configuracao

As constantes principais ficam em `settings.py`, incluindo:

- `INGEST_WORKBOOK_PATH`
- `INGEST_SHEET_NAME`
- `DICTIONARIES_SHEET_NAME`
- `INGEST_READY_STATUS`
- `OUTPUT_BASE`
- `RULES_BASE`
- `BATCH_SIZE`
- `CRS_WGS84`
- `CRS_EQUAL_AREA`
- `ENABLE_GROUP_CONSOLIDATION`
- `KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING`
- `USE_ARROW_IO`
- `INTERACTIVE_ATTRIBUTE_REVIEW`

Configuracoes por projeto ficam em `projects/configs.py`, e funcoes opcionais
ficam registradas em `projects/registry.py`.

## Testes

Executar a suite:

```powershell
uv run pytest
```

Ou:

```powershell
py -3.14 -m pytest
```

Validar especificamente os perfis de regras:

```powershell
py -3.14 -m unittest tests.test_rule_profiles_integration
```

## Documentacao

Documentacao complementar:

- `readme/README.md`: descricao operacional detalhada.
- `readme/rules.md`: contrato dos perfis modulares em `rules/`.

## Observacoes

- Os JSONs em `rules/` devem ser mantidos em UTF-8.
- Caminhos de regras, nomes de projeto, nomes de perfil e chaves em
  `projects/configs.py` devem permanecer em ASCII.
- Em ambientes nao interativos, mantenha `INTERACTIVE_ATTRIBUTE_REVIEW = False`.
- Se a entrada estiver em `EPSG:4326`, nao ha reprojecao desnecessaria.
- Em bases grandes, transformacoes espaciais sao feitas em fatias para reduzir
  risco de estouro de memoria.

