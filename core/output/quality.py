from dataclasses import dataclass

from core.geometry_repair import INTERNAL_SAFE_REPAIR_FLAG
from core.reporting import export_duplicate_reports
from core.spatial.spatial_functions import (
    get_geometric_duplicate_mask,
    get_geometric_duplicate_records,
    get_invalid_ogc_records,
)
from core.utils import log
from core.validation.validation_functions import (
    get_attribute_duplicate_mask,
    get_attribute_duplicate_records,
)
from settings import (
    ENABLE_ATTRIBUTE_DUPLICATE_REPORT,
    ENABLE_GEOMETRIC_DUPLICATE_REPORT,
    ENABLE_OGC_INVALID_REPORT,
)


@dataclass(frozen=True)
class OutputQualitySummary:
    attr_count: int
    geom_count: int
    ogc_invalid_count: int
    safe_null_count: int
    attr_report: str | None
    geom_report: str | None
    ogc_report: str | None
    ogc_error_summary: dict


def build_output_quality_summary(final_gdf, theme_output_dir, base_name):
    attr_count, attr_duplicates = _attribute_duplicates(final_gdf)
    geom_count, geom_duplicates = _geometric_duplicates(final_gdf)
    ogc_invalid, ogc_invalid_count, ogc_error_summary = _ogc_invalid(final_gdf)
    attr_report, geom_report, ogc_report = _export_reports_if_needed(
        final_gdf,
        theme_output_dir,
        base_name,
        attr_count,
        attr_duplicates,
        geom_count,
        geom_duplicates,
        ogc_invalid,
        ogc_invalid_count,
        ogc_error_summary,
    )

    return OutputQualitySummary(
        attr_count=attr_count,
        geom_count=geom_count,
        ogc_invalid_count=ogc_invalid_count,
        safe_null_count=_safe_null_count(final_gdf),
        attr_report=attr_report,
        geom_report=geom_report,
        ogc_report=ogc_report,
        ogc_error_summary=ogc_error_summary,
    )


def log_output_quality_summary(summary):
    if ENABLE_ATTRIBUTE_DUPLICATE_REPORT:
        log(f"Relatorio duplicados atributos: {summary.attr_report or 'nao gerado'}")
    else:
        log("Relatorio duplicados atributos: desabilitado em settings.py")

    if ENABLE_GEOMETRIC_DUPLICATE_REPORT:
        log(f"Relatorio duplicados geometricos: {summary.geom_report or 'nao gerado'}")
    else:
        log("Relatorio duplicados geometricos: desabilitado em settings.py")

    if ENABLE_OGC_INVALID_REPORT:
        if summary.ogc_report:
            log(f"Relatorio geometrias invalidas OGC: {summary.ogc_report}")
        else:
            log("Relatorio geometrias invalidas OGC: nao gerado")
    else:
        log("Relatorio geometrias invalidas OGC: desabilitado em settings.py")

    log(f"Total duplicados atributos: {summary.attr_count}")
    log(f"Total duplicados geometricos: {summary.geom_count}")
    log(f"Total geometrias invalidas OGC: {summary.ogc_invalid_count}")
    if summary.safe_null_count:
        log(f"Total geometrias nulas por reparo seguro: {summary.safe_null_count}")
    if ENABLE_OGC_INVALID_REPORT and summary.ogc_error_summary:
        log("Resumo erros OGC:")
        for erro, quantidade in summary.ogc_error_summary.items():
            log(f"  {quantidade}x - {erro}")


def _attribute_duplicates(final_gdf):
    if not ENABLE_ATTRIBUTE_DUPLICATE_REPORT:
        return 0, None

    attr_dup_mask = get_attribute_duplicate_mask(final_gdf)
    attr_count = int(attr_dup_mask.sum())
    if not attr_count:
        return attr_count, None

    return attr_count, get_attribute_duplicate_records(
        final_gdf,
        dup_mask=attr_dup_mask,
    )[0]


def _geometric_duplicates(final_gdf):
    if not ENABLE_GEOMETRIC_DUPLICATE_REPORT:
        return 0, None

    geom_dup_mask = get_geometric_duplicate_mask(final_gdf)
    geom_count = int(geom_dup_mask.sum())
    if not geom_count:
        return geom_count, None

    return geom_count, get_geometric_duplicate_records(
        final_gdf,
        dup_mask=geom_dup_mask,
    )[0]


def _ogc_invalid(final_gdf):
    if not ENABLE_OGC_INVALID_REPORT:
        return None, 0, {}
    return get_invalid_ogc_records(final_gdf)


def _export_reports_if_needed(
    final_gdf,
    theme_output_dir,
    base_name,
    attr_count,
    attr_duplicates,
    geom_count,
    geom_duplicates,
    ogc_invalid,
    ogc_invalid_count,
    ogc_error_summary,
):
    attr_report = None
    geom_report = None
    ogc_report = None

    if any(
        [
            ENABLE_ATTRIBUTE_DUPLICATE_REPORT and attr_count > 0,
            ENABLE_GEOMETRIC_DUPLICATE_REPORT and geom_count > 0,
            ENABLE_OGC_INVALID_REPORT and ogc_invalid_count > 0,
        ]
    ):
        (
            attr_report,
            geom_report,
            ogc_report,
            _,
            _,
            _,
            _,
        ) = export_duplicate_reports(
            final_gdf,
            theme_output_dir,
            base_name,
            attr_duplicates=attr_duplicates,
            attr_count=attr_count,
            geom_duplicates=geom_duplicates,
            geom_count=geom_count,
            ogc_invalid=ogc_invalid,
            ogc_invalid_count=ogc_invalid_count,
            ogc_error_summary=ogc_error_summary,
        )

    return attr_report, geom_report, ogc_report


def _safe_null_count(final_gdf):
    if INTERNAL_SAFE_REPAIR_FLAG not in final_gdf.columns:
        return 0
    return int(final_gdf[INTERNAL_SAFE_REPAIR_FLAG].fillna(False).sum())
