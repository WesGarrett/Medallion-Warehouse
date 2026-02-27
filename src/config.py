"""
Central configuration: database engine factory and path constants.
"""
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Load .env from the project root (two levels up from this file)
_PROJECT_ROOT = Path(__file__).parents[1]
load_dotenv(_PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
DATABASE_URL: str = os.environ["DATABASE_URL"]


def get_engine(echo: bool = False) -> Engine:
    """Return a connection-pooled SQLAlchemy engine."""
    return create_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=echo,
    )


# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------
RAW_DATA_DIR: Path = _PROJECT_ROOT / "data" / "raw"
SCHEMA_DIR: Path = Path(__file__).parent / "warehouse" / "schemas"
