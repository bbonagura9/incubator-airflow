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

import itertools

from sqlalchemy import (Column, Integer, String, Boolean)
from sqlalchemy import func

from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow.models import Base


class DagStat(Base):
    __tablename__ = "dag_stats"

    dag_id = Column(String(ID_LEN), primary_key=True)
    state = Column(String(50), primary_key=True)
    count = Column(Integer, default=0)
    dirty = Column(Boolean, default=False)

    def __init__(self, dag_id, state, count=0, dirty=False):
        self.dag_id = dag_id
        self.state = state
        self.count = count
        self.dirty = dirty

    @staticmethod
    @provide_session
    def set_dirty(dag_id, session=None):
        """
        :param dag_id: the dag_id to mark dirty
        :param session: database session
        :return:
        """
        DagStat.create(dag_id=dag_id, session=session)

        try:
            stats = session.query(DagStat).filter(
                DagStat.dag_id == dag_id
            ).with_for_update().all()

            for stat in stats:
                stat.dirty = True
            session.commit()
        except Exception as e:
            session.rollback()
            log = LoggingMixin().log
            log.warning("Could not update dag stats for %s", dag_id)
            log.exception(e)

    @staticmethod
    @provide_session
    def update(dag_ids=None, dirty_only=True, session=None):
        """
        Updates the stats for dirty/out-of-sync dags

        :param dag_ids: dag_ids to be updated
        :type dag_ids: list
        :param dirty_only: only updated for marked dirty, defaults to True
        :type dirty_only: bool
        :param session: db session to use
        :type session: Session
        """
        try:
            qry = session.query(DagStat)
            if dag_ids:
                qry = qry.filter(DagStat.dag_id.in_(set(dag_ids)))
            if dirty_only:
                qry = qry.filter(DagStat.dirty)

            qry = qry.with_for_update().all()

            ids = set([dag_stat.dag_id for dag_stat in qry])

            # avoid querying with an empty IN clause
            if len(ids) == 0:
                session.commit()
                return

            dagstat_states = set(itertools.product(ids, State.dag_states))
            qry = (
                session.query(DagRun.dag_id, DagRun.state, func.count('*'))
                .filter(DagRun.dag_id.in_(ids))
                .group_by(DagRun.dag_id, DagRun.state)
            )

            counts = {(dag_id, state): count for dag_id, state, count in qry}
            for dag_id, state in dagstat_states:
                count = 0
                if (dag_id, state) in counts:
                    count = counts[(dag_id, state)]

                session.merge(
                    DagStat(
                        dag_id=dag_id,
                        state=state,
                        count=count,
                        dirty=False))

            session.commit()
        except Exception as e:
            session.rollback()
            log = LoggingMixin().log
            log.warning("Could not update dag stat table")
            log.exception(e)

    @staticmethod
    @provide_session
    def create(dag_id, session=None):
        """
        Creates the missing states the stats table for the dag specified

        :param dag_id: dag id of the dag to create stats for
        :param session: database session
        :return:
        """
        # unfortunately sqlalchemy does not know upsert
        qry = session.query(DagStat).filter(DagStat.dag_id == dag_id).all()
        states = [dag_stat.state for dag_stat in qry]
        for state in State.dag_states:
            if state not in states:
                try:
                    session.merge(DagStat(dag_id=dag_id, state=state))
                    session.commit()
                except Exception as e:
                    session.rollback()
                    log = LoggingMixin().log
                    log.warning("Could not create stat record")
                    log.exception(e)
