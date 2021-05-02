from pathlib import Path

from .single_frame import DataSteps  # noqa: F401

with open(Path(__file__).parent / "VERSION") as f:
    __version__ = f.readline().strip()
