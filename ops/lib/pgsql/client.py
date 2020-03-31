# Copyright 2020 Canonical Ltd.
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

import re

from ...framework import EventBase, EventsBase, EventSource, StoredState
from ...model import ModelError, BlockedStatus, WaitingStatus

key_value_re = re.compile(r"""(?x)
                               (\w+) \s* = \s*
                               (?:
                                 (\S*)
                               )
                               (?=(?:\s|\Z))
                           """)


class PostgreSQLError(ModelError):
    """All errors raised by interface-pgsql will be subclasses of this error.

    It provides the attribute self.status to indicate what status and message the Unit should use
    based on this relation. (Eg, if there is no relation to PGSQl, it will raise a
    BlockedStatus('Missing relation <relation-name>')
    """

    def __init__(self, kind, message, relation_name):
        super().__init__()
        self.status = kind('{}: {}'.format(message, relation_name))


class PostgreSQLDatabase:

    def __init__(self, master):
        # This is a pgsql 'key=value key2=value' connection string
        self.master = master
        self.properties = {}
        for key, val in key_value_re.findall(master):
            if key not in self.properties:
                self.properties[key] = val

    @property
    def host(self):
        return self.properties['host']

    @property
    def database(self):
        return self.properties['dbname']

    @property
    def port(self):
        return self.properties['port']

    @property
    def user(self):
        return self.properties['user']

    @property
    def password(self):
        return self.properties['password']


class PostgreSQLMasterChanged(EventBase):

    def __init__(self, handle, master):
        super().__init__(handle)
        self.master = master

    def snapshot(self):
        return {'master': self.master}

    def restore(self, snapshot):
        self.master = snapshot['master']


class PostgreSQLEvents(EventsBase):
    master_changed = EventSource(PostgreSQLMasterChanged)


class PostgreSQLClient(EventsBase):
    """This provides a Client that understands how to communicate with the PostgreSQL Charm.

    The two primary methods are .master() which will give you the connection information for the
    current PostgreSQL master (or raise an error if the relation or master is not properly
    established yet).
    """
    on = PostgreSQLEvents()
    state = StoredState()

    def __init__(self, parent, name):
        if parent is None:
            raise RuntimeError('must pass a valid CharmBase')
        super().__init__(parent, name)
        self.name = name
        self.framework.observe(parent.on[self.name].relation_changed, self.on_relation_changed)
        self.framework.observe(parent.on[self.name].relation_broken, self.on_relation_broken)
        self.state.set_default(master=None, database=None, roles=None, extensions=None)

    def master(self):
        """Retrieve the libpq connection string for the Master postgresql database.

        This method will raise PostgreSQLError with a status of either Blocked or Waiting if the
        error does/doesn't need user intervention.
        """
        relations = self.framework.model.relations[self.name]
        if len(relations) == 1:
            if self.state.master is None:
                raise PostgreSQLError(WaitingStatus, 'master not ready yet', self.name)
            return PostgreSQLDatabase(self.state.master)
        if len(relations) == 0:
            raise PostgreSQLError(BlockedStatus, 'missing relation', self.name)
        if len(relations) > 1:
            raise PostgreSQLError(BlockedStatus, 'too many related applications', self.name)

    def standbys(self):
        """Retrieve the connection strings for all PostgreSQL standby machines."""
        pass

    def set_database_name(self, value):
        """Indicate the database that this charm wants to use."""

    def set_roles(self, value):
        """Indicate what roles you want available from PostgreSQL."""
        pass

    def set_extensions(self, value):
        """Indicate what extensions you want available from PostgreSQL."""
        pass

    def _resolve_master(self, new_master):
        # TODO: the pgsql charm likes to report that you can't actually connect as long as
        # local[egress-subnets] is not a subset of remote[allowed-subnets] and
        # the requested database, roles and extensions all match the values provided by remote
        pass

    def on_relation_changed(self, event):
        # Check to see if the master is now at a different location
        relation = event.relation
        data = relation.data[event.unit]
        # TODO: do we check if any related units have a 'master' set?
        #  Also, we need to check if we actually have the database, roles, and access that we want
        master = data.get('master')
        if master is not None:
            should_emit = self.state.master != master
        if should_emit:
            self.state.master = master
            self.on.master_changed.emit(master)

    def on_relation_broken(self, event):
        pass
