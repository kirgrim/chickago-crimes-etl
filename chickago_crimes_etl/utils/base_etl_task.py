import os
from abc import ABC


class BaseETLTask(ABC):

    DIM_FILES_DIR = os.getenv('DIM_FILES_DIR')
