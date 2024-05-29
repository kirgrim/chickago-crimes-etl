from typing import Type

from chickago_crimes_etl.processors.base import TrafficCrashesCSVProcessor


def run_processor(loader_cls: Type[TrafficCrashesCSVProcessor], run_id: str):
    loader = loader_cls()
    return loader.run(run_id=run_id)


def run_loader(processor_cls: Type[TrafficCrashesCSVProcessor], run_id: str):
    processor = processor_cls()
    return processor.run(run_id=run_id)
