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

from sqlalchemy import (Column, Integer, String, Text, ForeignKey)
from sqlalchemy.orm import relationship
from sqlalchemy_utc import UtcDateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class KnownEventType(Base):
    __tablename__ = "known_event_type"

    id = Column(Integer, primary_key=True)
    know_event_type = Column(String(200))

    def __repr__(self):
        return self.know_event_type


class KnownEvent(Base):
    __tablename__ = "known_event"

    id = Column(Integer, primary_key=True)
    label = Column(String(200))
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    user_id = Column(Integer(), ForeignKey('users.id'),)
    known_event_type_id = Column(Integer(), ForeignKey('known_event_type.id'),)
    reported_by = relationship(
        "User", cascade=False, cascade_backrefs=False, backref='known_events')
    event_type = relationship(
        "KnownEventType",
        cascade=False,
        cascade_backrefs=False, backref='known_events')
    description = Column(Text)

    def __repr__(self):
        return self.label
