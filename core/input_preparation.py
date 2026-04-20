from core.dataset_io import read_input_dataset
from core.ingest_loader import validate_theme_and_attributes
from core.transforms.attribute_transforms import clean_whitespace, normalize_columns
from core.utils import log
from settings import INGEST_READY_STATUS, INGEST_SHEET_NAME, INGEST_WORKBOOK_PATH


def log_dataset_overview(gdf):
    columns = list(gdf.columns)
    log(f"Atributos encontrados: {len(columns)}")
    for column in columns:
        log(f"  - {column} ({gdf[column].dtype})")


def log_queue_summary(summary, issues):
    log("Resumo da planilha ingest:")
    log(f"  Aba analisada: {INGEST_SHEET_NAME}")
    log(f"  Caminho da planilha: {INGEST_WORKBOOK_PATH}")
    log(f"  Registros lidos: {summary['total_records']}")
    log(f"  Status elegivel: {INGEST_READY_STATUS}")
    log(f"  Registros com status elegivel: {summary['ready_candidates']}")
    log(f"  Arquivos aptos para processamento: {summary['eligible_records']}")
    log(f"  Registros ignorados com excecao: {summary['issues']}")

    if issues:
        log("Excecoes encontradas na fila ingest:")
        for issue in issues:
            log(
                "  "
                f"Linha {issue.sheet_row} | ID={issue.record_id} | "
                f"theme_folder={issue.theme_folder or '<vazio>'} | "
                f"motivo={issue.reason}"
            )


def log_dictionary_validation(record, input_attributes):
    result = validate_theme_and_attributes(record.theme, input_attributes)

    if not result["theme_found"]:
        log(
            f"Theme sem correspondencia na aba dictionaries: '{record.theme}'. "
            "Validacao estrutural nao executada."
        )
        return

    if not result["missing_attributes"] and not result["extra_attributes"]:
        log(
            f"Validacao dictionaries OK para theme '{result['dictionary_theme']}'. "
            "Estrutura do arquivo compativel com original_attribute_name."
        )
        return

    log(
        f"Divergencias estruturais encontradas para theme '{result['dictionary_theme']}'."
    )
    if result["missing_attributes"]:
        log(f"  Campos ausentes no arquivo: {', '.join(result['missing_attributes'])}")
    if result["extra_attributes"]:
        log(f"  Campos excedentes no arquivo: {', '.join(result['extra_attributes'])}")


def load_and_prepare_input(record):
    gdf = read_input_dataset(record.input_path)
    input_attributes = list(gdf.columns)
    log_dictionary_validation(record, input_attributes)

    gdf = normalize_columns(gdf)
    gdf = clean_whitespace(gdf)
    return gdf
