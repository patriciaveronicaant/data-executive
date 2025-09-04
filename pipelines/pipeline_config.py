from dataclasses import dataclass

@dataclass #decorator - store ng data
class PipelineConfig:
    #Configuration for the data pipeline
    source_path: str
    target_connection: str
    target_table: str
    batch_size: int = 1000
    max_retries: int = 3
    data_quality_checks: bool = True