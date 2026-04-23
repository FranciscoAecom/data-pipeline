from dataclasses import dataclass


@dataclass(frozen=True)
class ProcessingExecutionContext:
    record: object
    output_dir: str
    project_config: dict
    rule_profile_name: str
    rule_profile: dict | None
    optional_functions: dict
    id_start: int = 1
