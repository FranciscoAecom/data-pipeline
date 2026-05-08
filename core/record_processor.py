from core.processing.result import ProcessRecordResult
from core.processing_service import ProcessingService


def process_record(
    record,
    output_dir,
    id_start=1,
    use_configured_final_name=False,
    persist_individual_output=True,
):
    service = ProcessingService()
    return service.process(
        record,
        output_dir,
        id_start=id_start,
        use_configured_final_name=use_configured_final_name,
        persist_individual_output=persist_individual_output,
    )
