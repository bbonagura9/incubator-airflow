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


import json
import pickle


from sqlalchemy import (
    Column, Integer, String, DateTime,
    Index, LargeBinary)
from sqlalchemy import and_
from sqlalchemy_utc import UtcDateTime
from sqlalchemy.ext.declarative import declarative_base

from airflow import configuration

from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.helpers import as_tuple
from airflow.utils.log.logging_mixin import LoggingMixin

Base = declarative_base()


class XCom(Base, LoggingMixin):
    """
    Base class for XCom objects.
    """
    __tablename__ = "xcom"

    id = Column(Integer, primary_key=True)
    key = Column(String(512))
    value = Column(LargeBinary)
    timestamp = Column(
        DateTime, default=timezone.utcnow, nullable=False)
    execution_date = Column(UtcDateTime, nullable=False)

    # source information
    task_id = Column(String(ID_LEN), nullable=False)
    dag_id = Column(String(ID_LEN), nullable=False)

    __table_args__ = (
        Index(
            'idx_xcom_dag_task_date',
            dag_id,
            task_id,
            execution_date,
            unique=False),
    )

    def __repr__(self):
        return '<XCom "{key}" ({task_id} @ {execution_date})>'.format(
            key=self.key,
            task_id=self.task_id,
            execution_date=self.execution_date)

    @classmethod
    @provide_session
    def set(
            cls,
            key,
            value,
            execution_date,
            task_id,
            dag_id,
            enable_pickling=None,
            session=None):
        """
        Store an XCom value.
        TODO: "pickling" has been deprecated and JSON is preferred. "pickling" will be
        removed in Airflow 2.0. :param enable_pickling: If pickling is not enabled, the
        XCOM value will be parsed as JSON instead.

        :return: None
        """
        session.expunge_all()

        if enable_pickling is None:
            enable_pickling = configuration.getboolean(
                'core', 'enable_xcom_pickling')

        if enable_pickling:
            value = pickle.dumps(value)
        else:
            try:
                value = json.dumps(value).encode('UTF-8')
            except ValueError:
                log = LoggingMixin().log
                log.error("Could not serialize the XCOM value into JSON. "
                          "If you are using pickles instead of JSON "
                          "for XCOM, then you need to enable pickle "
                          "support for XCOM in your airflow config.")
                raise

        # remove any duplicate XComs
        session.query(cls).filter(
            cls.key == key,
            cls.execution_date == execution_date,
            cls.task_id == task_id,
            cls.dag_id == dag_id).delete()

        session.commit()

        # insert new XCom
        session.add(XCom(
            key=key,
            value=value,
            execution_date=execution_date,
            task_id=task_id,
            dag_id=dag_id))

        session.commit()

    @classmethod
    @provide_session
    def get_one(
            cls,
            execution_date,
            key=None,
            task_id=None,
            dag_id=None,
            include_prior_dates=False,
            enable_pickling=None,
            session=None):
        """
        Retrieve an XCom value, optionally meeting certain criteria.
        TODO: "pickling" has been deprecated and JSON is preferred. "pickling" will be removed in Airflow 2.0.

        :param enable_pickling: If pickling is not enabled, the XCOM value will be parsed to JSON instead.
        :return: XCom value
        """
        filters = []
        if key:
            filters.append(cls.key == key)
        if task_id:
            filters.append(cls.task_id == task_id)
        if dag_id:
            filters.append(cls.dag_id == dag_id)
        if include_prior_dates:
            filters.append(cls.execution_date <= execution_date)
        else:
            filters.append(cls.execution_date == execution_date)

        query = (
            session.query(cls.value)
            .filter(and_(*filters))
            .order_by(cls.execution_date.desc(), cls.timestamp.desc()))

        result = query.first()
        if result:
            if enable_pickling is None:
                enable_pickling = configuration.getboolean(
                    'core', 'enable_xcom_pickling')

            if enable_pickling:
                return pickle.loads(result.value)
            else:
                try:
                    return json.loads(result.value.decode('UTF-8'))
                except ValueError:
                    log = LoggingMixin().log
                    log.error("Could not serialize the XCOM value into JSON. "
                              "If you are using pickles instead of JSON "
                              "for XCOM, then you need to enable pickle "
                              "support for XCOM in your airflow config.")
                    raise

    @classmethod
    @provide_session
    def get_many(
            cls,
            execution_date,
            key=None,
            task_ids=None,
            dag_ids=None,
            include_prior_dates=False,
            limit=100,
            enable_pickling=None,
            session=None):
        """
        Retrieve an XCom value, optionally meeting certain criteria
        TODO: "pickling" has been deprecated and JSON is preferred. "pickling" will be removed in Airflow 2.0.
        """
        filters = []
        if key:
            filters.append(cls.key == key)
        if task_ids:
            filters.append(cls.task_id.in_(as_tuple(task_ids)))
        if dag_ids:
            filters.append(cls.dag_id.in_(as_tuple(dag_ids)))
        if include_prior_dates:
            filters.append(cls.execution_date <= execution_date)
        else:
            filters.append(cls.execution_date == execution_date)

        query = (
            session.query(cls)
            .filter(and_(*filters))
            .order_by(cls.execution_date.desc(), cls.timestamp.desc())
            .limit(limit))
        results = query.all()
        if enable_pickling is None:
            enable_pickling = configuration.getboolean(
                'core', 'enable_xcom_pickling')
        for result in results:
            if enable_pickling:
                result.value = pickle.loads(result.value)
            else:
                try:
                    result.value = json.loads(result.value.decode('UTF-8'))
                except ValueError:
                    log = LoggingMixin().log
                    log.error("Could not serialize the XCOM value into JSON. "
                              "If you are using pickles instead of JSON "
                              "for XCOM, then you need to enable pickle "
                              "support for XCOM in your airflow config.")
                    raise
        return results

    @classmethod
    @provide_session
    def delete(cls, xcoms, session=None):
        if isinstance(xcoms, XCom):
            xcoms = [xcoms]
        for xcom in xcoms:
            if not isinstance(xcom, XCom):
                raise TypeError(
                    'Expected XCom; received {}'.format(
                        xcom.__class__.__name__))
            session.delete(xcom)
        session.commit()
