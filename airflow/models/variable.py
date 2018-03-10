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
from builtins import object, bytes

import json


from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, Float, LargeBinary)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym


from airflow.exceptions import AirflowException

from airflow.utils.db import provide_session
from airflow.utils.helpers import (
    as_tuple, is_container, is_in, validate_key, pprinttable)
from airflow.utils.log.logging_mixin import LoggingMixin


class Variable(Base, LoggingMixin):
    __tablename__ = "variable"

    id = Column(Integer, primary_key=True)
    key = Column(String(ID_LEN), unique=True)
    _val = Column('val', Text)
    is_encrypted = Column(Boolean, unique=False, default=False)

    def __repr__(self):
        # Hiding the value
        return '{} : {}'.format(self.key, self._val)

    def get_val(self):
        if self._val and self.is_encrypted:
            try:
                fernet = get_fernet()
            except:
                raise AirflowException(
                    "Can't decrypt _val for key={}, FERNET_KEY configuration \
                    missing".format(self.key))
            try:
                return fernet.decrypt(bytes(self._val, 'utf-8')).decode()
            except:
                raise AirflowException(
                    "Can't decrypt _val for key={}, invalid token or value"
                    .format(self.key))
        else:
            return self._val

    def set_val(self, value):
        if value:
            try:
                fernet = get_fernet()
                self._val = fernet.encrypt(bytes(value, 'utf-8')).decode()
                self.is_encrypted = True
            except AirflowException:
                self.log.exception(
                    "Failed to load fernet while encrypting value, using non-encrypted value."
                )
                self._val = value
                self.is_encrypted = False

    @declared_attr
    def val(cls):
        return synonym('_val',
                       descriptor=property(cls.get_val, cls.set_val))

    @classmethod
    def setdefault(cls, key, default, deserialize_json=False):
        """
        Like a Python builtin dict object, setdefault returns the current value
        for a key, and if it isn't there, stores the default value and returns it.

        :param key: Dict key for this Variable
        :type key: String
        :param default: Default value to set and return if the variable
        isn't already in the DB
        :type default: Mixed
        :param deserialize_json: Store this as a JSON encoded value in the DB
         and un-encode it when retrieving a value
        :return: Mixed
        """
        default_sentinel = object()
        obj = Variable.get(key, default_var=default_sentinel, deserialize_json=deserialize_json)
        if obj is default_sentinel:
            if default is not None:
                Variable.set(key, default, serialize_json=deserialize_json)
                return default
            else:
                raise ValueError('Default Value must be set')
        else:
            return obj

    @classmethod
    @provide_session
    def get(cls, key, default_var=None, deserialize_json=False, session=None):
        obj = session.query(cls).filter(cls.key == key).first()
        if obj is None:
            if default_var is not None:
                return default_var
            else:
                raise KeyError('Variable {} does not exist'.format(key))
        else:
            if deserialize_json:
                return json.loads(obj.val)
            else:
                return obj.val

    @classmethod
    @provide_session
    def set(cls, key, value, serialize_json=False, session=None):

        if serialize_json:
            stored_value = json.dumps(value)
        else:
            stored_value = str(value)

        session.query(cls).filter(cls.key == key).delete()
        session.add(Variable(key=key, val=stored_value))
        session.flush()
