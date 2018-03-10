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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from future.standard_library import install_aliases

from builtins import str
from builtins import object, bytes
import copy
from collections import namedtuple
from datetime import timedelta

import dill
import functools
import getpass
import imp
import importlib
import itertools
import zipfile
import jinja2
import json
import logging
import os
import pickle
import re
import signal
import sys
import textwrap
import traceback
import warnings
import hashlib

from datetime import datetime
from urllib.parse import urlparse, quote

from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, Float, LargeBinary)
from sqlalchemy import func, or_, and_
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import reconstructor, relationship, synonym
from sqlalchemy_utc import UtcDateTime

from croniter import croniter
import six

from airflow import settings, utils
from airflow.executors import GetDefaultExecutor, LocalExecutor
from airflow import configuration
from airflow.exceptions import AirflowException, AirflowSkipException, AirflowTaskTimeout
from airflow.dag.base_dag import BaseDag, BaseDagBag
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.ti_deps.deps.task_concurrency_dep import TaskConcurrencyDep

from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, RUN_DEPS
from airflow.utils import timezone
from airflow.utils.dates import cron_presets, date_range as utils_date_range
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.email import send_email
from airflow.utils.helpers import (
    as_tuple, is_container, is_in, validate_key, pprinttable)
from airflow.utils.operator_resources import Resources
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weight_rule import WeightRule
from airflow.utils.net import get_hostname
from airflow.utils.log.logging_mixin import LoggingMixin


class SkipMixin(LoggingMixin):
    @provide_session
    def skip(self, dag_run, execution_date, tasks, session=None):
        """
        Sets tasks instances to skipped from the same dag run.

        :param dag_run: the DagRun for which to set the tasks to skipped
        :param execution_date: execution_date
        :param tasks: tasks to skip (not task_ids)
        :param session: db session to use
        """
        if not tasks:
            return

        task_ids = [d.task_id for d in tasks]
        now = timezone.utcnow()

        if dag_run:
            session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_run.dag_id,
                TaskInstance.execution_date == dag_run.execution_date,
                TaskInstance.task_id.in_(task_ids)
            ).update({TaskInstance.state : State.SKIPPED,
                      TaskInstance.start_date: now,
                      TaskInstance.end_date: now},
                     synchronize_session=False)
            session.commit()
        else:
            assert execution_date is not None, "Execution date is None and no dag run"

            self.log.warning("No DAG RUN present this should not happen")
            # this is defensive against dag runs that are not complete
            for task in tasks:
                ti = TaskInstance(task, execution_date=execution_date)
                ti.state = State.SKIPPED
                ti.start_date = now
                ti.end_date = now
                session.merge(ti)

            session.commit()


@functools.total_ordering
