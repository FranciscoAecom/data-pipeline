# Data_Pipeline

Aplicacao para validacao e transformacao de arquivos geoespaciais em lote, orientada pela planilha de ingest.

## Visao Geral

O projeto processa arquivos `.shp` e `.gpkg` a partir da planilha:

- `input/st_Ingest_parameter.xlsx`
- aba `datas`

Uma linha entra na esteira quando:

- `status = Waiting Update`
- `path_shapefile_temp` aponta para um `.shp`, `.gpkg` ou pasta com esses arquivos
- o caminho nao aponta para `.zip`
- `theme_folder` encontra um perfil em `rules/`

As saidas ficam em `output/<theme_folder>/`.

## Regras

O perfil de regra e obrigatorio.

Exemplo:

- `theme_folder = app_car_es`
- perfil esperado: `rules/app_car/app_car_es/`

Associacao entre `theme_folder` e `rules`:

- o projeto e inferido pelo prefixo do `theme_folder`
- `app_car_*` usa `rules/app_car/`
- `rl_car_*` usa `rules/reserva_legal_car/`
- `estado` usa `rules/estado/`
- `auth_supn` usa `rules/autorizacao_para_supressao_vegetal/`
- se o perfil esperado nao existir, a linha nao e processada e o log informa o caminho esperado
- se houver mais de uma rule com o mesmo nome final, o pipeline falha a associacao para evitar ambiguidade
- quando informado dentro do JSON, o campo `theme_folder` precisa bater com o nome final do perfil

O sistema normaliza a comparacao de nomes:

- remove espacos excedentes
- converte espacos para `_`
- compara em minusculas
- aceita perfis em subpastas, como `app_car/app_car_es`

Se existirem valores com motivo `Valor fora do dominio configurado.`, o projeto pode atualizar automaticamente o JSON ativo com:

- novos `accepted_values`
- `aliases`
- `relations`

Os perfis em `rules/` usam estrutura modular. Cada perfil fica em uma pasta com `profile.json`, `input_schema.json`, `domains.json`, `relations.json` e `pipeline.json`. Veja `readme/rules.md` para o formato completo e use `rules/_template/` como base para novos perfis.

Os JSONs em `rules/` devem ser mantidos em UTF-8. A suite de testes verifica se algum perfil contem sinais comuns de texto quebrado, como `Ã`, `Â` ou `�`, para evitar valores como `AutorizaÃ§Ã£o` em dominios aceitos.

## Dictionaries

Antes das transformacoes, o projeto valida a estrutura do arquivo contra a aba `dictionaries` da mesma planilha.

Fluxo:

1. le `theme` na aba `datas`
2. procura o mesmo `theme` na aba `dictionaries`
3. compara os atributos do arquivo com `original_attribute_name`
4. registra em log campos ausentes e excedentes

Para `.gpkg`, a validacao usa a primeira layer encontrada.

## Entrada e Saida

Entradas suportadas:

- `.shp`
- `.gpkg`

Comportamento:

- se `path_shapefile_temp` apontar para um arquivo, processa esse arquivo
- se apontar para uma pasta, processa todos os `.shp` e `.gpkg` encontrados nela e nas subpastas
- `.zip` nao e processado

Saida:

- arquivo principal sempre em `.gpkg`
- relatorios auxiliares na mesma pasta do resultado
- log contextual `.txt` com a mesma base de nome do `gpkg`
- consolidado por grupo quando `ENABLE_GROUP_CONSOLIDATION = True`
- quando `ENABLE_GROUP_CONSOLIDATION = True` e `KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING = False`, o projeto grava apenas o consolidado final do grupo
- em reexecucoes, o `.gpkg` de saida pode ser sobrescrito com seguranca no inicio da escrita

## Colunas e Geometria

Convencoes:

- colunas originais permanecem em `sdb_*`
- colunas tratadas saem em `acm_*`
- o arquivo final nao grava marcacoes tecnicas internas

Fluxo de schema:

1. durante a carga e preparo, os nomes de entrada sao normalizados para `sdb_*`
2. validacoes e transformacoes genericas do `core` devem ler o atributo original em `sdb_*`
3. qualquer normalizacao, derivacao ou padronizacao deve produzir uma coluna `acm_*`
4. funcoes genericas nao devem sobrescrever `sdb_*`; o valor original da base deve ser preservado

Geometria:

- o pipeline achata geometrias para 2D antes dos calculos espaciais
- quando houver `Z` ou dimensoes superiores, apenas `X/Y` sao mantidos

### BBox regional CAR

Para bases dos projetos `app_car` e `reserva_legal_car`, o pipeline executa uma validacao adicional de bounding box por UF depois do reparo de geometrias invalidas e antes do complemento das metricas espaciais.

Motivacao:

- a validacao global de coordenadas verifica limites do mundo (`longitude -180..180`, `latitude -90..90`)
- alguns erros podem passar por essa validacao global, por exemplo uma geometria de uma base do Maranhao com parte em latitude `83`
- apesar de latitude `83` ser valida globalmente, ela e invalida para a UF esperada e distorce o bbox final do GeoPackage/GeoServer

Comportamento:

- a UF e inferida por `theme_folder`, `rule_profile`, `input_path`, `source_path` ou `theme`
- por enquanto, a regra fica restrita aos projetos `app_car` e `reserva_legal_car`
- o pipeline compara as geometrias com um envelope esperado para a UF
- geometrias que extrapolam esse envelope sao recortadas para o bbox da UF
- registros nao sao removidos
- quando houver recorte, as metricas `acm_a_ha`, `acm_prm_km`, `acm_long` e `acm_lat` sao recalculadas para os registros alterados
- se uma geometria estiver fora do envelope e nao intersectar a UF, ela nao e alterada; o caso e registrado em log

Exemplo de log quando ocorre correcao:

```text
Validacao de bbox regional CAR
BBox regional CAR: 1 geometria(s) recortada(s) para o envelope da UF MA.
Validacao de bbox regional CAR concluido em 0.12s
```

Exemplo de log quando uma geometria fora do envelope nao intersecta a UF:

```text
BBox regional CAR: 1 geometria(s) fora do envelope da UF MA nao foram alteradas porque nao intersectam o estado.
```

O codigo dessa regra fica em `core/spatial/regional_bounds.py`.

## Estrutura

Raiz:

- `main.py`: ponto de entrada
- `settings.py`: configuracao central

Core:

- `core/ingest_loader.py`: leitura da ingest e validacao estrutural
- `core/dataset_io.py`: leitura e escrita geoespacial
- `core/schema.py`: convencoes de colunas `sdb_*` e `acm_*`
- `core/text.py`: parsing e normalizacao generica de texto
- `core/date/`: tratamento generico de campos de data
- `core/pipeline.py`: pipeline principal de transformacoes
- `core/batch_processor.py`: processamento em lotes
- `core/reporting.py`: relatorios auxiliares
- `core/naming.py`: nomes de saida e caminhos por `theme_folder`
- `core/optional_functions.py`: funcoes opcionais genericas e integracao com projetos
- `core/transforms/attribute_transforms.py`: transformacoes de schema e atributos
- `core/spatial/spatial_functions.py`: operacoes espaciais e validacao OGC
- `core/spatial/regional_bounds.py`: validacao e correcao de bbox regional para `app_car` e `reserva_legal_car`
- `core/validation/rule_engine.py`: fachada publica para regras e perfis
- `core/validation/rule_loader.py`: carregamento, listagem e persistencia de perfis
- `core/validation/rule_validation.py`: validacao estrutural dos perfis e componentes
- `core/validation/domain_rules.py`: classificacao e mapeamento de valores de dominio
- `core/validation/rule_normalization.py`: normalizacao de nomes e textos de regras
- `core/validation/tabular_schema.py`: leitura do contrato tabular de entrada
- `core/validation/tabular_validation.py`: validacao de colunas, tipos e geometria
- `core/validation/tabular_coercion.py`: normalizacao/conversao de tipos antes da validacao
- `core/validation/rule_autofix.py`: autoajuste de dominio
- `core/validation/validation_functions.py`: validacoes tabulares e consistencia entre campos
- `core/helper_unique_values.py`: exportacao de valores unicos
- `core/processing_service.py`: servico de orquestracao do processamento por arquivo
- `core/utils.py`: log contextual

Projetos:

- `projects/configs.py`: configuracao por projeto
- `projects/registry.py`: registro explicito das funcoes opcionais por projeto
- `projects/functions/`: funcoes especificas por regra de negocio; logica generica deve ficar em `core/`

Dados:

- `rules/`: perfis JSON modulares
- `output/`: resultados por `theme_folder`

Arvore resumida:

```text
Data_Pipeline/
  main.py
  settings.py
  core/
    ingest_loader.py
    dataset_io.py
    schema.py
    text.py
    pipeline.py
    batch_processor.py
    reporting.py
    naming.py
    date/
      __init__.py
      date.py
    optional_functions.py
    helper_unique_values.py
    utils.py
    transforms/
      attribute_transforms.py
    spatial/
      spatial_functions.py
    validation/
      rule_engine.py
      rule_autofix.py
      validation_functions.py
  projects/
    configs.py
    functions/
      app_car.py
  rules/
    app_car/
      app_car_ac/
        profile.json
        input_schema.json
        domains.json
        relations.json
        pipeline.json
  output/
```

## Configuracao

Em `settings.py`, as constantes mais importantes sao:

- `INGEST_WORKBOOK_PATH`
- `INGEST_SHEET_NAME`
- `DICTIONARIES_SHEET_NAME`
- `INGEST_READY_STATUS`
- `OUTPUT_BASE`
- `RULES_BASE`
- `DEFAULT_RULE_PROFILE`
- `BATCH_SIZE`
- `CRS_WGS84`
- `CRS_EQUAL_AREA`
- `ENABLE_ATTRIBUTE_DUPLICATE_REPORT`
- `ENABLE_GEOMETRIC_DUPLICATE_REPORT`
- `ENABLE_OGC_INVALID_REPORT`
- `ENABLE_GROUP_CONSOLIDATION`
- `KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING`
- `SPATIAL_TRANSFORM_CHUNK_SIZE`
- `USE_ARROW_IO`
- `INTERACTIVE_ATTRIBUTE_REVIEW`

Configuracoes por projeto ficam em `projects/configs.py`.

Exemplo de configuracao por projeto:

- `output_name_template`
- `reference_date`
- funcoes opcionais registradas em `projects/functions/`

## Como Usar

Ambiente recomendado:

1. crie um ambiente com Python 3.14
2. instale as dependencias com `py -3.14 -m pip install -e .`

Exemplo no Windows:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -3.14 -m pip install --upgrade pip
py -3.14 -m pip install -e .
```

Depois:

1. atualize `input/st_Ingest_parameter.xlsx`
2. defina `status = Waiting Update` na aba `datas`
3. ajuste `path_shapefile_temp`, `theme_folder` e `theme`
4. garanta que o perfil correspondente exista em `rules/`
5. execute `py -3.14 main.py`
6. consulte `output/<theme_folder>/`

## Logs e Relatorios

O projeto registra:

- resumo inicial da fila
- excecoes de elegibilidade
- validacao estrutural contra `dictionaries`
- progresso por batch
- tempo por etapa principal, como carga da base, processamento, reparo geometrico e persistencia
- correcao de bbox regional CAR quando alguma geometria extrapola a UF esperada nos projetos habilitados
- resumo final das validacoes
- atualizacao automatica de perfil quando aplicavel
- caminhos dos arquivos gerados

Relatorios opcionais:

- duplicados por atributos
- duplicados geometricos
- geometrias invalidas OGC
- `*_inconsistencias_dominio.xlsx`

O relatorio de inconsistencias de dominio usa `core/helper_unique_values.py`.

## Observacoes

- o fluxo continua aplicando `auto_functions` definidas no perfil
- nomes tecnicos devem permanecer em ASCII:
  caminhos de regras, nomes de projeto, nomes de perfil e chaves em `projects/configs.py`
- se o perfil nao existir em `rules/`, a base nao e processada
- a validacao estrutural acontece antes da normalizacao de atributos
- se uma pasta tiver varios arquivos suportados, todos entram na fila
- a layer do `gpkg` acompanha automaticamente o nome do arquivo
- se a entrada ja estiver em `EPSG:4326`, nao ha reprojecao
- calculos e reprojecoes espaciais usam fatias menores para reduzir risco de estouro de memoria em bases grandes
- geometrias invalidas sao reparadas antes da gravacao final; quando o reparo seguro nao for possivel, o log registra a ocorrencia
- para `app_car` e `reserva_legal_car`, geometrias que extrapolam o bbox esperado da UF podem ser recortadas sem remocao de registros
- o script `core/helper_unique_values.py` tambem pode ser usado manualmente
- em Python 3.14, a leitura com Arrow e tentada primeiro; se o ambiente nao suportar, o projeto volta automaticamente para a leitura padrao do `pyogrio`
- em ambientes nao interativos, deixe `INTERACTIVE_ATTRIBUTE_REVIEW = False` para evitar prompts durante a execucao do pipeline
