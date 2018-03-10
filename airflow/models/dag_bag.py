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


from builtins import str
from collections import namedtuple
from datetime import timedelta

import imp
import importlib
import zipfile
import os
import re
import sys
import textwrap
import hashlib

from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, Float, LargeBinary)
from sqlalchemy import or_


from airflow import settings
from airflow.executors import GetDefaultExecutor
from airflow import configuration
from airflow.dag.base_dag import BaseDagBag

from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.helpers import (
    as_tuple, is_container, is_in, validate_key, pprinttable)
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from airflow.utils.log.logging_mixin import LoggingMixin


class DagBag(BaseDagBag, LoggingMixin):
    """
    A dagbag is a collection of dags, parsed out of a folder tree and has high
    level configuration settings, like what database to use as a backend and
    what executor to use to fire off tasks. This makes it easier to run
    distinct environments for say production and development, tests, or for
    different teams or security profiles. What would have been system level
    settings are now dagbag level so that one system can run multiple,
    independent settings sets.

    :param dag_folder: the folder to scan to find DAGs
    :type dag_folder: unicode
    :param executor: the executor to use when executing task instances
        in this DagBag
    :param include_examples: whether to include the examples that ship
        with airflow or not
    :type include_examples: bool
    """

    def __init__(
        self,
        dag_folder=None,
        executor=None,
        include_examples=configuration.getboolean(
            'core',
            'LOAD_EXAMPLES')):

        # do not use default arg in signature, to fix import cycle on plugin
        # load
        if executor is None:
            executor = GetDefaultExecutor()
        dag_folder = dag_folder or settings.DAGS_FOLDER
        self.log.info("Filling up the DagBag from %s", dag_folder)
        self.dag_folder = dag_folder
        self.dags = {}
        # the file's last modified timestamp when we last read it
        self.file_last_changed = {}
        self.executor = executor
        self.import_errors = {}

        if include_examples:
            example_dag_folder = os.path.join(
                os.path.dirname(__file__),
                'example_dags')
            self.collect_dags(example_dag_folder)
        self.collect_dags(dag_folder)

    def size(self):
        """
        :return: the amount of dags contained in this dagbag
        """
        return len(self.dags)

    def get_dag(self, dag_id):
        """
        Gets the DAG out of the dictionary, and refreshes it if expired
        """
        # If asking for a known subdag, we want to refresh the parent
        root_dag_id = dag_id
        if dag_id in self.dags:
            dag = self.dags[dag_id]
            if dag.is_subdag:
                root_dag_id = dag.parent_dag.dag_id

        # If the dag corresponding to root_dag_id is absent or expired
        orm_dag = DagModel.get_current(root_dag_id)
        if orm_dag and (
                root_dag_id not in self.dags or
                (
                    orm_dag.last_expired and
                    dag.last_loaded < orm_dag.last_expired
                )
        ):
            # Reprocess source file
            found_dags = self.process_file(
                filepath=orm_dag.fileloc, only_if_updated=False)

            # If the source file no longer exports `dag_id`, delete it from
            # self.dags
            if found_dags and dag_id in [dag.dag_id for dag in found_dags]:
                return self.dags[dag_id]
            elif dag_id in self.dags:
                del self.dags[dag_id]
        return self.dags.get(dag_id)

    def process_file(self, filepath, only_if_updated=True, safe_mode=True):
        """
        Given a path to a python module or zip file, this method imports
        the module and look for dag objects within it.
        """
        found_dags = []

        # if the source file no longer exists in the DB or in the filesystem,
        # return an empty list
        # todo: raise exception?
        if filepath is None or not os.path.isfile(filepath):
            return found_dags

        try:
            # This failed before in what may have been a git sync
            # race condition
            file_last_changed_on_disk = datetime.fromtimestamp(
                os.path.getmtime(filepath))
            if only_if_updated \
                    and filepath in self.file_last_changed \
                    and file_last_changed_on_disk == self.file_last_changed[filepath]:
                return found_dags

        except Exception as e:
            self.log.exception(e)
            return found_dags

        mods = []
        if not zipfile.is_zipfile(filepath):
            if safe_mode and os.path.isfile(filepath):
                with open(filepath, 'rb') as f:
                    content = f.read()
                    if not all([s in content for s in (b'DAG', b'airflow')]):
                        self.file_last_changed[filepath] = file_last_changed_on_disk
                        return found_dags

            self.log.debug("Importing %s", filepath)
            org_mod_name, _ = os.path.splitext(os.path.split(filepath)[-1])
            mod_name = ('unusual_prefix_' +
                        hashlib.sha1(filepath.encode('utf-8')).hexdigest() +
                        '_' + org_mod_name)

            if mod_name in sys.modules:
                del sys.modules[mod_name]

            with timeout(configuration.getint('core', "DAGBAG_IMPORT_TIMEOUT")):
                try:
                    m = imp.load_source(mod_name, filepath)
                    mods.append(m)
                except Exception as e:
                    self.log.exception("Failed to import: %s", filepath)
                    self.import_errors[filepath] = str(e)
                    self.file_last_changed[filepath] = file_last_changed_on_disk

        else:
            zip_file = zipfile.ZipFile(filepath)
            for mod in zip_file.infolist():
                head, _ = os.path.split(mod.filename)
                mod_name, ext = os.path.splitext(mod.filename)
                if not head and (ext == '.py' or ext == '.pyc'):
                    if mod_name == '__init__':
                        self.log.warning(
                            "Found __init__.%s at root of %s", ext, filepath)
                    if safe_mode:
                        with zip_file.open(mod.filename) as zf:
                            self.log.debug(
                                "Reading %s from %s", mod.filename, filepath)
                            content = zf.read()
                            if not all(
                                    [s in content for s in (b'DAG', b'airflow')]):
                                self.file_last_changed[filepath] = (
                                    file_last_changed_on_disk)
                                # todo: create ignore list
                                return found_dags

                    if mod_name in sys.modules:
                        del sys.modules[mod_name]

                    try:
                        sys.path.insert(0, filepath)
                        m = importlib.import_module(mod_name)
                        mods.append(m)
                    except Exception as e:
                        self.log.exception("Failed to import: %s", filepath)
                        self.import_errors[filepath] = str(e)
                        self.file_last_changed[filepath] = file_last_changed_on_disk

        for m in mods:
            for dag in list(m.__dict__.values()):
                if isinstance(dag, DAG):
                    if not dag.full_filepath:
                        dag.full_filepath = filepath
                        if dag.fileloc != filepath:
                            dag.fileloc = filepath
                    dag.is_subdag = False
                    self.bag_dag(dag, parent_dag=dag, root_dag=dag)
                    found_dags.append(dag)
                    found_dags += dag.subdags

        self.file_last_changed[filepath] = file_last_changed_on_disk
        return found_dags

    @provide_session
    def kill_zombies(self, session=None):
        """
        Fails tasks that haven't had a heartbeat in too long
        """
        from airflow.jobs import LocalTaskJob as LJ
        self.log.info("Finding 'running' jobs without a recent heartbeat")
        TI = TaskInstance
        secs = configuration.getint(
            'scheduler', 'scheduler_zombie_task_threshold')
        limit_dttm = timezone.utcnow() - timedelta(seconds=secs)
        self.log.info("Failing jobs without heartbeat after %s", limit_dttm)

        tis = (
            session.query(TI)
            .join(LJ, TI.job_id == LJ.id)
            .filter(TI.state == State.RUNNING)
            .filter(
                or_(
                    LJ.state != State.RUNNING,
                    LJ.latest_heartbeat < limit_dttm,
                ))
            .all()
        )

        for ti in tis:
            if ti and ti.dag_id in self.dags:
                dag = self.dags[ti.dag_id]
                if ti.task_id in dag.task_ids:
                    task = dag.get_task(ti.task_id)
                    ti.task = task
                    ti.handle_failure("{} killed as zombie".format(str(ti)))
                    self.log.info('Marked zombie job %s as failed', ti)
                    Stats.incr('zombies_killed')
        session.commit()

    def bag_dag(self, dag, parent_dag, root_dag):
        """
        Adds the DAG into the bag, recurses into sub dags.
        """
        self.dags[dag.dag_id] = dag
        dag.resolve_template_files()
        dag.last_loaded = timezone.utcnow()

        for task in dag.tasks:
            settings.policy(task)

        for subdag in dag.subdags:
            subdag.full_filepath = dag.full_filepath
            subdag.parent_dag = dag
            subdag.is_subdag = True
            self.bag_dag(subdag, parent_dag=dag, root_dag=root_dag)
        self.log.debug('Loaded DAG {dag}'.format(**locals()))

    def collect_dags(
            self,
            dag_folder=None,
            only_if_updated=True):
        """
        Given a file path or a folder, this method looks for python modules,
        imports them and adds them to the dagbag collection.

        Note that if a .airflowignore file is found while processing,
        the directory, it will behaves much like a .gitignore does,
        ignoring files that match any of the regex patterns specified
        in the file.
        """
        start_dttm = timezone.utcnow()
        dag_folder = dag_folder or self.dag_folder

        # Used to store stats around DagBag processing
        stats = []
        FileLoadStat = namedtuple(
            'FileLoadStat', "file duration dag_num task_num dags")
        if os.path.isfile(dag_folder):
            self.process_file(dag_folder, only_if_updated=only_if_updated)
        elif os.path.isdir(dag_folder):
            patterns = []
            for root, dirs, files in os.walk(dag_folder, followlinks=True):
                ignore_file = [f for f in files if f == '.airflowignore']
                if ignore_file:
                    f = open(os.path.join(root, ignore_file[0]), 'r')
                    patterns += [p for p in f.read().split('\n') if p]
                    f.close()
                for f in files:
                    try:
                        filepath = os.path.join(root, f)
                        if not os.path.isfile(filepath):
                            continue
                        mod_name, file_ext = os.path.splitext(
                            os.path.split(filepath)[-1])
                        if file_ext != '.py' and not zipfile.is_zipfile(
                                filepath):
                            continue
                        if not any(
                                [re.findall(p, filepath) for p in patterns]):
                            ts = timezone.utcnow()
                            found_dags = self.process_file(
                                filepath, only_if_updated=only_if_updated)

                            td = timezone.utcnow() - ts
                            td = td.total_seconds() + (
                                float(td.microseconds) / 1000000)
                            stats.append(FileLoadStat(
                                filepath.replace(dag_folder, ''),
                                td,
                                len(found_dags),
                                sum([len(dag.tasks) for dag in found_dags]),
                                str([dag.dag_id for dag in found_dags]),
                            ))
                    except Exception as e:
                        self.log.exception(e)
        Stats.gauge(
            'collect_dags',
            (timezone.utcnow() - start_dttm).total_seconds(),
            1)
        Stats.gauge(
            'dagbag_size', len(self.dags), 1)
        Stats.gauge(
            'dagbag_import_errors', len(self.import_errors), 1)
        self.dagbag_stats = sorted(
            stats, key=lambda x: x.duration, reverse=True)

    def dagbag_report(self):
        """Prints a report around DagBag loading stats"""
        report = textwrap.dedent("""\n
        -------------------------------------------------------------------
        DagBag loading stats for {dag_folder}
        -------------------------------------------------------------------
        Number of DAGs: {dag_num}
        Total task number: {task_num}
        DagBag parsing time: {duration}
        {table}
        """)
        stats = self.dagbag_stats
        return report.format(
            dag_folder=self.dag_folder,
            duration=sum([o.duration for o in stats]),
            dag_num=sum([o.dag_num for o in stats]),
            task_num=sum([o.task_num for o in stats]),
            table=pprinttable(stats),
        )

    @provide_session
    def deactivate_inactive_dags(self, session=None):
        active_dag_ids = [dag.dag_id for dag in list(self.dags.values())]
        for dag in session.query(
                DagModel).filter(~DagModel.dag_id.in_(active_dag_ids)).all():
            dag.is_active = False
            session.merge(dag)
        session.commit()

    @provide_session
    def paused_dags(self, session=None):
        dag_ids = [dp.dag_id for dp in session.query(DagModel).filter(
            DagModel.is_paused.__eq__(True))]
        return dag_ids
