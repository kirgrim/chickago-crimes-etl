from typing import Type

from chickago_crimes_etl.processors.preprocessor import TrafficCrashesPreprocessor
from chickago_crimes_etl.processors.base import TrafficCrashesCSVProcessor
from chickago_crimes_etl.loaders.base import TrafficCrashesLoader


def run_preprocessor(run_id: str):
    preprocessor = TrafficCrashesPreprocessor()
    return preprocessor.run(run_id=run_id)


def run_processor(processor_cls: Type[TrafficCrashesCSVProcessor], run_id: str):
    loader = processor_cls()
    return loader.run(run_id=run_id)


def run_loader(loader_cls: Type[TrafficCrashesLoader], run_id: str):
    processor = loader_cls()
    return processor.run(run_id=run_id)
