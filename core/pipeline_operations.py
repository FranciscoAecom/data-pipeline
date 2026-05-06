from dataclasses import dataclass
from typing import Callable, Literal


OperationKind = Literal["validation", "transform", "normalization", "spatial"]


@dataclass(frozen=True)
class PipelineOperation:
    name: str
    kind: OperationKind
    handler: Callable
    source_column: str | None = None
    target_column: str | None = None

    def execute(self, gdf, column, **context):
        return self.handler(gdf, column, **context)


def build_pipeline_operation(func_name, handler, source_column=None):
    return PipelineOperation(
        name=func_name,
        kind=infer_operation_kind(func_name),
        handler=handler,
        source_column=source_column,
        target_column=_target_column(source_column, func_name),
    )


def infer_operation_kind(func_name):
    name = str(func_name)
    if name.startswith("validate_"):
        return "validation"
    if "transform" in name:
        return "transform"
    if "normaliz" in name:
        return "normalization"
    if "spatial" in name or "geometry" in name or "bbox" in name:
        return "spatial"
    return "transform"


def _target_column(source_column, func_name):
    if not source_column or infer_operation_kind(func_name) == "validation":
        return None
    if str(source_column).startswith("sdb_"):
        return f"acm_{source_column[4:]}"
    return f"acm_{source_column}"
