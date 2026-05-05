# Regras Modulares

Cada perfil em `rules/` deve ficar em uma pasta com cinco arquivos:

```text
rules/<projeto>/<perfil>/
  profile.json
  input_schema.json
  domains.json
  relations.json
  pipeline.json
```

Use `rules/_template/` como ponto de partida para uma nova base.

## profile.json

Metadados do perfil.

```json
{
  "profile_name": "rl_car_ac",
  "project_name": "reserva_legal_car",
  "theme_folder": "rl_car_ac",
  "description": "Regras para Reserva Legal do CAR AC."
}
```

Campos obrigatorios:

- `profile_name`: nome do perfil.
- `project_name`: projeto responsavel pelo perfil.
- `theme_folder`: deve bater com o nome final da pasta do perfil.

## input_schema.json

Valida a estrutura tabular da base lida antes do processamento principal.

```json
{
  "columns": {
    "sdb_cod_tema": {
      "dtype": "string",
      "required": true,
      "nullable": false
    }
  },
  "require_geometry": true,
  "allow_extra_columns": true
}
```

Tipos aceitos em `dtype`:

- `string`, `str`, `text`
- `number`, `numeric`
- `float`, `double`
- `integer`, `int`
- `datetime`, `date`
- `boolean`, `bool`

Observacoes:

- As colunas sao validadas depois da normalizacao para `sdb_*`.
- Quando o tipo lido nao bate com `dtype`, o pipeline tenta converter a coluna automaticamente e registra a conversao no log.
- Se a conversao gerar novos nulos em uma coluna com `nullable: false`, o processamento para com erro.
- Use `required: false` para colunas que podem nao existir na entrada.
- Use `nullable: false` quando a coluna nao pode conter valores nulos.
- Datas que chegam como texto devem ficar como `string`; a conversao ocorre depois por `validate_date_fields`.

## domains.json

Define dominios aceitos e aliases por coluna.

```json
{
  "fields": {
    "sdb_cod_tema": {
      "accepted_values": [
        "ARL_AVERBADA"
      ],
      "aliases": {
        "ARL AVERBADA": "ARL_AVERBADA"
      }
    }
  }
}
```

`accepted_values` deve ser uma lista de strings. `aliases` deve mapear valores alternativos para um valor canonico existente em `accepted_values`.

## relations.json

Define consistencia entre campos.

```json
{
  "relations": {
    "cod_tema_to_nom_tema": {
      "ARL_AVERBADA": "Reserva Legal Averbada"
    }
  }
}
```

O nome da relacao segue o padrao `<origem>_to_<destino>`. O pipeline resolve esses tokens para colunas como `sdb_cod_tema` e `sdb_nom_tema`.

## pipeline.json

Define funcoes opcionais aplicadas em cada coluna.

```json
{
  "auto_functions": {
    "sdb_cod_tema": [
      "validate_shapefile_attribute"
    ]
  }
}
```

Funcoes podem ser nomes curtos registrados em `projects/registry.py` ou nomes qualificados como `pacote.modulo.funcao`.

## Validacao

O carregador valida cada componente separadamente e depois consolida tudo em memoria no formato usado pelo pipeline:

```json
{
  "profile_name": "...",
  "project_name": "...",
  "theme_folder": "...",
  "input_schema": {},
  "fields": {},
  "relations": {},
  "auto_functions": {}
}
```

Para verificar os perfis:

```powershell
py -3.14 -m unittest tests.test_rule_profiles_integration
```
