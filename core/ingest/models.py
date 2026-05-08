from dataclasses import dataclass


@dataclass
class IngestRecord:
    sheet_row: int
    record_id: object
    theme: str
    theme_folder: str
    status: str
    source_path: str
    input_path: str
    rule_profile: str


@dataclass
class IngestIssue:
    sheet_row: int
    record_id: object
    theme_folder: str
    status: str
    source_path: str
    reason: str


__all__ = ["IngestIssue", "IngestRecord"]
