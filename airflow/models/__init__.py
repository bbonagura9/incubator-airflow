# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from sqlalchemy.ext.declarative import declarative_base

from airflow import settings, configuration
from airflow.exceptions import AirflowException
from airflow.models.base_operator import BaseOperator  # noqa
from airflow.models.chart import Chart  # noqa
from airflow.models.connection import Connection  # noqa
from airflow.models.dag import DAG  # noqa
from airflow.models.dag_bag import DagBag  # noqa
from airflow.models.dag_model import DagModel  # noqa
from airflow.models.dag_pickle import DagPickle  # noqa
from airflow.models.dag_run import DagRun  # noqa
from airflow.models.dag_stat import DagStat  # noqa
from airflow.models.import_error import ImportError  # noqa
from airflow.models.known_event import KnownEvent  # noqa
from airflow.models.known_event_type import KnownEventType  # noqa
from airflow.models.log import Log  # noqa
from airflow.models.pool import Pool  # noqa
from airflow.models.skip_mixin import SkipMixin  # noqa
from airflow.models.sla_miss import SlaMiss  # noqa
from airflow.models.task_fail import TaskFail  # noqa
from airflow.models.task_instance import TaskInstance  # noqa
from airflow.models.user import User  # noqa
from airflow.models.variable import Variable  # noqa
from airflow.models.xcom import XCom  # noqa
from airflow.utils import timezone
from airflow.utils.state import State

ID_LEN = 250
XCOM_RETURN_KEY = 'return_value'

Stats = settings.Stats
Base = declarative_base()


def clear_task_instances(tis, session, activate_dag_runs=True, dag=None):
    """
    Clears a set of task instances, but makes sure the running ones
    get killed.
    """
    job_ids = []
    for ti in tis:
        if ti.state == State.RUNNING:
            if ti.job_id:
                ti.state = State.SHUTDOWN
                job_ids.append(ti.job_id)
        else:
            task_id = ti.task_id
            if dag and dag.has_task(task_id):
                task = dag.get_task(task_id)
                task_retries = task.retries
                ti.max_tries = ti.try_number + task_retries - 1
            else:
                # Ignore errors when updating max_tries if dag is None or
                # task not found in dag since database records could be
                # outdated. We make max_tries the maximum value of its
                # original max_tries or the current task try number.
                ti.max_tries = max(ti.max_tries, ti.try_number - 1)
            ti.state = State.NONE
            session.merge(ti)

    if job_ids:
        from airflow.jobs import BaseJob as BJ
        for job in session.query(BJ).filter(BJ.id.in_(job_ids)).all():
            job.state = State.SHUTDOWN

    if activate_dag_runs and tis:
        drs = session.query(DagRun).filter(
            DagRun.dag_id.in_({ti.dag_id for ti in tis}),
            DagRun.execution_date.in_({ti.execution_date for ti in tis}),
        ).all()
        for dr in drs:
            dr.state = State.RUNNING
            dr.start_date = timezone.utcnow()


def get_fernet():
    """
    Deferred load of Fernet key.

    This function could fail either because Cryptography is not installed
    or because the Fernet key is invalid.

    :return: Fernet object
    :raises: AirflowException if there's a problem trying to load Fernet
    """
    try:
        from cryptography.fernet import Fernet
    except ImportError:
        raise AirflowException('Failed to import Fernet, it may not be installed')
    try:
        return Fernet(configuration.get('core', 'FERNET_KEY').encode('utf-8'))
    except (ValueError, TypeError) as ve:
        raise AirflowException("Could not create Fernet object: {}".format(ve))
