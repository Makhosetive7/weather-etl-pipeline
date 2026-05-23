"""Pipeline run metadata tracking in weather.etl_runs."""

import logging
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config.config import Config

logger = logging.getLogger(__name__)


class EtlRunTracker:
    def __init__(self, engine=None):
        self.engine = engine or create_engine(Config.DATABASE_URL)

    def start_run(self, run_id: Optional[str] = None) -> str:
        run_id = run_id or str(uuid.uuid4())
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO weather.etl_runs (run_id, status)
                        VALUES (:run_id, 'running')
                        """
                    ),
                    {"run_id": run_id},
                )
                conn.commit()
            logger.info(f"ETL run started: {run_id}")
        except SQLAlchemyError as e:
            logger.warning(f"Could not record ETL run start (table may be missing): {e}")
        return run_id

    def finish_run(
        self,
        run_id: str,
        status: str,
        *,
        records_extracted: int = 0,
        records_transformed: int = 0,
        records_loaded: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE weather.etl_runs
                        SET finished_at = :finished_at,
                            status = :status,
                            records_extracted = :records_extracted,
                            records_transformed = :records_transformed,
                            records_loaded = :records_loaded,
                            records_failed = :records_failed,
                            error_message = :error_message
                        WHERE run_id = :run_id
                        """
                    ),
                    {
                        "run_id": run_id,
                        "finished_at": datetime.now(),
                        "status": status,
                        "records_extracted": records_extracted,
                        "records_transformed": records_transformed,
                        "records_loaded": records_loaded,
                        "records_failed": records_failed,
                        "error_message": error_message,
                    },
                )
                conn.commit()
            logger.info(f"ETL run finished: {run_id} status={status}")
        except SQLAlchemyError as e:
            logger.warning(f"Could not record ETL run finish: {e}")

    def close(self):
        self.engine.dispose()
