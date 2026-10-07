from __future__ import annotations

import os
from typing import Sequence

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection


def get_postgres_connection() -> connection:
    load_dotenv(override=True)

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DBNAME", "arrise_vm_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD") or None,
        connect_timeout=15,
    )


def get_open_job_applications(
    job_eids: Sequence[str],
) -> pd.DataFrame:
    normalized_job_eids = [
        str(job_eid).strip()
        for job_eid in job_eids
        if job_eid is not None and str(job_eid).strip()
    ]
    normalized_job_eids = list(dict.fromkeys(normalized_job_eids))

    output_columns = [
        "app_id",
        "job_eid",
        "requisition_id",
        "app_workflow_state_name",
        "app_full_name",
        "app_country",
        "app_sourcetype",
        "job_title",
        "app_hire_date",
    ]

    if not normalized_job_eids:
        print("No valid open job_eids were provided.")
        return pd.DataFrame(columns=output_columns)

    placeholders = ", ".join(["%s"] * len(normalized_job_eids))

    query = f"""
        SELECT DISTINCT
            app_id,
            job_eid,
            requisition_id,
            state_name AS app_workflow_state_name,
            full_name AS app_full_name,
            country AS app_country,
            source_type AS app_sourcetype,
            title AS job_title,
            NULL::date AS app_hire_date
        FROM jv_arrise_data_schema.applications
        WHERE job_eid IN ({placeholders})
          AND app_id IS NOT NULL
          AND COALESCE(TRIM(state_name), '') NOT ILIKE '%%reject%%'
          AND COALESCE(TRIM(state_name), '') NOT ILIKE '%%withdraw%%'
        ORDER BY
            job_title,
            app_full_name,
            app_workflow_state_name;
    """

    connection_ = None

    try:
        connection_ = get_postgres_connection()

        return pd.read_sql_query(
            sql=query,
            con=connection_,
            params=tuple(normalized_job_eids),
        )
    except Exception as error:
        raise RuntimeError(
            f"Error retrieving applications from PostgreSQL: {error}"
        ) from error
    finally:
        if connection_ is not None:
            connection_.close()
