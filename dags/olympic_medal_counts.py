"""Airflow DAG for counting Olympic medals in MySQL."""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

MYSQL_CONN_ID = "olympic_mysql"
MEDALS = ("Bronze", "Silver", "Gold")
MAX_RECORD_AGE_SECONDS = 30


def choose_random_medal() -> str:
    """Choose one medal type and publish it through the task's XCom result."""
    medal = random.choice(MEDALS)
    logging.info("Randomly selected medal: %s", medal)
    return medal


def choose_medal_branch(**context: object) -> str:
    """Return the task ID matching the medal selected by choose_medal."""
    task_instance = context["ti"]
    selected_medal = task_instance.xcom_pull(task_ids="choose_medal")
    branch_by_medal = {
        "Bronze": "count_bronze_medals",
        "Silver": "count_silver_medals",
        "Gold": "count_gold_medals",
    }

    if selected_medal not in branch_by_medal:
        raise ValueError(f"Unsupported medal selected: {selected_medal!r}")

    selected_task = branch_by_medal[selected_medal]
    logging.info("Following branch %s for %s", selected_task, selected_medal)
    return selected_task


def sleep_before_check(**context: object) -> None:
    """Delay validation to demonstrate both successful and failed sensor runs."""
    delay_seconds = int(context["params"]["delay_seconds"])
    logging.info("Sleeping for %s second(s) before freshness validation", delay_seconds)
    time.sleep(delay_seconds)


def latest_medal_count_is_fresh() -> bool:
    """Return True when the newest medal_counts row is at most 30 seconds old."""
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    latest_record = hook.get_first(
        """
        SELECT
            id,
            medal_type,
            `count`,
            created_at,
            TIMESTAMPDIFF(MICROSECOND, created_at, UTC_TIMESTAMP(6)) / 1000000
                AS age_seconds
        FROM olympic_dataset.medal_counts
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """
    )

    if latest_record is None:
        logging.warning("The medal_counts table has no records yet")
        return False

    record_id, medal_type, medal_count, created_at, age_seconds = latest_record
    age_seconds = float(age_seconds)
    logging.info(
        "Latest row: id=%s, medal_type=%s, count=%s, created_at=%s, age=%.3fs",
        record_id,
        medal_type,
        medal_count,
        created_at,
        age_seconds,
    )
    return 0 <= age_seconds <= MAX_RECORD_AGE_SECONDS


with DAG(
    dag_id="olympic_medal_counts",
    description="Count a randomly selected Olympic medal and validate row freshness",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "delay_seconds": Param(
            default=5,
            type="integer",
            minimum=0,
            maximum=300,
            description="Delay before the freshness sensor; use 35 to make it fail.",
        )
    },
    tags=["goit", "homework-07", "mysql"],
) as dag:
    create_medal_counts_table = SQLExecuteQueryOperator(
        task_id="create_medal_counts_table",
        conn_id=MYSQL_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS olympic_dataset.medal_counts (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                medal_type VARCHAR(10) NOT NULL,
                `count` BIGINT UNSIGNED NOT NULL,
                created_at DATETIME(6) NOT NULL,
                PRIMARY KEY (id),
                INDEX idx_medal_counts_created_at (created_at)
            ) ENGINE=InnoDB
              DEFAULT CHARSET=utf8mb4
              COLLATE=utf8mb4_unicode_ci
        """,
    )

    choose_medal = PythonOperator(
        task_id="choose_medal",
        python_callable=choose_random_medal,
    )

    branch_by_medal = BranchPythonOperator(
        task_id="branch_by_medal",
        python_callable=choose_medal_branch,
    )

    count_bronze_medals = SQLExecuteQueryOperator(
        task_id="count_bronze_medals",
        conn_id=MYSQL_CONN_ID,
        sql="""
            INSERT INTO olympic_dataset.medal_counts
                (medal_type, `count`, created_at)
            SELECT 'Bronze', COUNT(*), UTC_TIMESTAMP(6)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze'
        """,
    )

    count_silver_medals = SQLExecuteQueryOperator(
        task_id="count_silver_medals",
        conn_id=MYSQL_CONN_ID,
        sql="""
            INSERT INTO olympic_dataset.medal_counts
                (medal_type, `count`, created_at)
            SELECT 'Silver', COUNT(*), UTC_TIMESTAMP(6)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver'
        """,
    )

    count_gold_medals = SQLExecuteQueryOperator(
        task_id="count_gold_medals",
        conn_id=MYSQL_CONN_ID,
        sql="""
            INSERT INTO olympic_dataset.medal_counts
                (medal_type, `count`, created_at)
            SELECT 'Gold', COUNT(*), UTC_TIMESTAMP(6)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold'
        """,
    )

    delay_before_freshness_check = PythonOperator(
        task_id="delay_before_freshness_check",
        python_callable=sleep_before_check,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_latest_record_freshness = PythonSensor(
        task_id="check_latest_record_freshness",
        python_callable=latest_medal_count_is_fresh,
        poke_interval=1,
        timeout=7,
        mode="poke",
        soft_fail=False,
    )

    medal_count_tasks = [
        count_bronze_medals,
        count_silver_medals,
        count_gold_medals,
    ]

    create_medal_counts_table >> choose_medal >> branch_by_medal
    branch_by_medal >> medal_count_tasks
    medal_count_tasks >> delay_before_freshness_check >> check_latest_record_freshness
