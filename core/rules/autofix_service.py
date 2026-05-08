import os

from core.naming import build_theme_output_dir
from core.rules.autofix import autofix_rule_profile_from_invalid_domains
from core.utils import log, timed_log_step


class RuleAutofixService:
    def autofix_rule_profile(self, context, final_gdf):
        with timed_log_step("Ajuste automatico do perfil de regras"):
            support_report_path = self.build_support_report_path(context)

            try:
                return autofix_rule_profile_from_invalid_domains(
                    context.rule_profile_name,
                    context.rule_profile,
                    final_gdf,
                    support_report_path=support_report_path,
                )
            except Exception as exc:
                log(f"Erro ao tentar corrigir automaticamente o perfil de regras: {exc}")
                return None

    def build_support_report_path(self, context):
        theme_output_dir = build_theme_output_dir(
            context.output_dir,
            context.record.theme_folder,
        )
        os.makedirs(theme_output_dir, exist_ok=True)
        base_name = os.path.splitext(os.path.basename(context.record.input_path))[0]
        return os.path.join(
            theme_output_dir,
            f"{base_name}_inconsistencias_dominio.xlsx",
        )

    def log_autofix_summary(self, summary):
        if not summary or not summary["changed"]:
            return

        log("Inconsistencias de dominio detectadas. Perfil de regras atualizado automaticamente.")
        log(f"Perfil atualizado: {summary['profile_path']}")
        if summary["invalid_columns"]:
            log(f"Atributos analisados para ajuste: {', '.join(summary['invalid_columns'])}")
        if summary["report_path"]:
            log(f"Relatorio de apoio com valores unicos: {summary['report_path']}")
        for column, values in summary["accepted_values_added"].items():
            log(f"  Novos valores aceitos em {column}: {', '.join(values)}")
        for column, aliases in summary["aliases_added"].items():
            alias_parts = [f"{source} -> {target}" for source, target in aliases.items()]
            log(f"  Novos aliases em {column}: {', '.join(alias_parts)}")
        for relation_key, mapping in summary["relations_added"].items():
            relation_parts = [f"{source} -> {target}" for source, target in mapping.items()]
            log(f"  Novas relacoes em {relation_key}: {', '.join(relation_parts)}")
        log(
            "As novas regras foram salvas para as proximas execucoes. "
            "Reprocesse a base se quiser aplicar o ajuste automaticamente neste mesmo arquivo."
        )


__all__ = ["RuleAutofixService"]
