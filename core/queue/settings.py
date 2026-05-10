from dataclasses import dataclass

from settings import (
    ENABLE_GROUP_CONSOLIDATION,
    KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING,
    OUTPUT_BASE,
)


@dataclass(frozen=True)
class QueueRunSettings:
    output_base: object = OUTPUT_BASE
    enable_group_consolidation: bool = ENABLE_GROUP_CONSOLIDATION
    keep_individual_outputs_when_grouping: bool = KEEP_INDIVIDUAL_OUTPUTS_WHEN_GROUPING

    @classmethod
    def from_output_base(cls, output_base=None):
        return cls(output_base=OUTPUT_BASE if output_base is None else output_base)
