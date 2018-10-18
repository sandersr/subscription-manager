# -*- coding: utf-8 -*-

from __future__ import print_function, division, absolute_import
#
# Copyright (c) 2018 Red Hat, Inc.
#
# This software is licensed to you under the GNU General Public License,
# version 2 (GPLv2). There is NO WARRANTY for this software, express or
# implied, including the implied warranties of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. You should have received a copy of GPLv2
# along with this software; if not, see
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.
#
# Red Hat trademarks are not licensed under GPLv2. No permission is
# granted to use or replicate Red Hat trademarks that are incorporated
# in this software or its documentation.

"""
This module contains utilities for manipulating files pertaining to system syspurpose
"""

import json
import os
import io
from syspurpose.utils import system_exit, create_dir, create_file, make_utf8, write_to_file_utf8
from syspurpose.i18n import ugettext as _
from syspurpose.sync import three_way_merge

# Constants for locations of the two system syspurpose files
USER_SYSPURPOSE = "/etc/rhsm/syspurpose/syspurpose.json"
VALID_FIELDS = "/etc/rhsm/syspurpose/valid_fields.json"  # Will be used for future validation
CACHED_SYSPURPOSE = "/var/lib/rhsm/cache/syspurpose.json" # Stores cached values

# All names that represent syspurpose values locally
ROLE = 'role'
ADDONS = 'addons'
SERVICE_LEVEL = 'service_level_agreement'
USAGE = 'usage'

# Remote values keyed on the local ones
LOCAL_TO_REMOTE = {
    ROLE: 'role',
    ADDONS: 'addOns',
    SERVICE_LEVEL: 'serviceLevel',
    USAGE: 'usage'
}

# All known syspurpose attributes
ATTRIBUTES = [ROLE, ADDONS, SERVICE_LEVEL, USAGE]


class SyspurposeStore(object):
    """
    Represents and maintains a json syspurpose file
    """

    def __init__(self, path, raise_on_error=False):
        self.path = path
        self.contents = {}
        self.raise_on_error = raise_on_error

    def read_file(self):
        """
        Opens & reads the contents of the store's file based on the 'path' provided to the constructor,
        and stores them on this object. If the user doesn't have access rights to the file, the program exits.
        :return: False if the contents of the file were empty, or the file doesn't exist; otherwise, nothing.
        """
        try:
            with io.open(self.path, 'r', encoding='utf-8') as f:
                self.contents = json.load(f, encoding='utf-8')
                return True
        except ValueError:
            # Malformed JSON or empty file. Let's not error out on an empty file
            if os.path.getsize(self.path):
                system_exit(os.EX_CONFIG,
                    _("Error: Malformed data in file {}; please review and correct.").format(self.path))

            return False
        except OSError as e:
            if e.errno == os.errno.EACCES and not self.raise_on_error:
                system_exit(os.EX_NOPERM,
                    _('Cannot read syspurpose file {}\nAre you root?').format(self.path))

            if self.raise_on_error:
                raise e
        except IOError as ioerr:
            if ioerr.errno == os.errno.ENOENT:
                return False
            if self.raise_on_error:
                raise ioerr

    def create(self):
        """
        Create the files necessary for this store
        :return: True if changes were made, false otherwise
        """
        return create_dir(os.path.dirname(self.path)) or \
            self.read_file() or \
            create_file(self.path, self.contents)

    def add(self, key, value):
        """
        Add a value to a list of values specified by key. If the current value specified by the key is scalar/non-list,
        it is not overridden, but maintained in the list, along with the new value.
        :param key: The name of the list
        :param value: The value to append to the list
        :return: None
        """
        value = make_utf8(value)
        key = make_utf8(key)
        try:
            current_value = self.contents[key]
            if current_value is not None and not isinstance(current_value, list):
                self.contents[key] = [current_value]

            if self.contents[key] is None:
                self.contents[key] = []

            if value not in self.contents[key]:
                self.contents[key].append(value)
            else:
                return False
        except (AttributeError, KeyError):
            self.contents[key] = [value]
        return True

    def remove(self, key, value):
        """
        Remove a value from a list specified by key.
        If the current value specified by the key is not a list, unset the value.
        :param key: The name of the list parameter to manipulate
        :param value: The value to attempt to remove
        :return: True if the value was in the list, False if it was not
        """
        value = make_utf8(value)
        key = make_utf8(key)
        try:
            current_value = self.contents[key]
            if current_value is not None and not isinstance(current_value, list) and current_value == value:
                return self.unset(key)

            if value in current_value:
                self.contents[key].remove(value)
            else:
                return False

            return True
        except (AttributeError, KeyError, ValueError):
            return False

    def unset(self, key):
        """
        Unsets a key
        :param key: The key to unset
        :return: boolean
        """
        key = make_utf8(key)

        # Special handling is required for the SLA, since it deviates from the typical CP
        # empty => null semantics
        if key == 'service_level_agreement':
            value = self.contents.get(key, None)
            self.contents[key] = ''
        else:
            value = self.contents.pop(key, None)

        return value is not None

    def set(self, key, value):
        """
        Set a key (syspurpose parameter) to value
        :param key: The parameter of the syspurpose file to set
        :type key: str

        :param value: The value to set that parameter to
        :return: Whether any change was made
        """
        value = make_utf8(value)
        key = make_utf8(key)
        org = make_utf8(self.contents.get(key, None))
        self.contents[key] = value
        return org != value or org is None

    def write(self, fp=None):
        """
        Write the current contents to the file at self.path
        """
        if not fp:
            with io.open(self.path, 'w', encoding='utf-8') as f:
                write_to_file_utf8(f, self.contents)
                f.flush()
        else:
            write_to_file_utf8(fp, self.contents)

    @classmethod
    def read(cls, path, raise_on_error=False):
        """
        Read the file represented by path. If the file does not exist it is created.
        :param path: The path on the file system to read, should be a json file
        :param raise_on_error: When it is set to True, then exceptions are raised as expected.
        :return: new SyspurposeStore with the contents read in
        """
        new_store = cls(path, raise_on_error=raise_on_error)

        if not os.access(path, os.W_OK):
            new_store.create()
        else:
            new_store.read_file()

        return new_store


class SyncedStore(object):
    """
    Stores values in a local file backed by a cache which is then synced with another source
    of the same values.
    """
    PATH = ""
    CACHE_PATH = ""

    def __init__(self, uep, on_changed=None):
        self.uep = uep
        self.filename = self.PATH.split('/')[-1]
        self.path = self.PATH
        self.cache_path = self.CACHE_PATH
        self.local_file = None
        self.local_contents = {}
        self.cache_file = None
        self.cache_contents = {}
        self.changed = False
        self.on_changed = on_changed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finsh()

    def start(self):
        self.local_file = open(self.path, 'w', encoding='utf-8')
        self.cache_file = open(self.path, 'w', encoding='utf-8')
        return self

    def finish(self):
        if self.changed:
            self.sync()
        self.local_file.close()
        self.cache_file.close()

    def sync(self):
        local_contents = self.get_local_contents()
        remote_contents = self.get_remote_contents()
        cached_contents = self.get_cached_contents()

        result = self.merge(local=local_contents,
                            remote=remote_contents,
                            base=cached_contents)

        self.update_local(result)
        self.update_cache(result)
        self.update_remote(result)

        # Reset the changed attribute as all items should be synced if we've gotten to this point
        self.changed = False

        return result

    def merge(self, local=None, remote=None, base=None):
        return three_way_merge(local=local, base=base, remote=remote,
                                     on_change=self.on_changed)

    def get_local_contents(self):
        raise NotImplemented("To be implemented in subclasses")

    def get_remote_contents(self):
        raise NotImplemented("To be implemented in subclasses")

    def get_cached_contents(self):
        raise NotImplemented("To be implemented in subclasses")

    def update_local(self, data):
        write_to_file_utf8(self.local_file, data)

    def update_cache(self, data):
        write_to_file_utf8(self.cache_file, data)

    def update_remote(self, data):
        raise NotImplemented("To be implemented in subclasses")

    def add(self, key, value):
        """
        Add a value to a list of values specified by key. If the current value specified by the key is scalar/non-list,
        it is not overridden, but maintained in the list, along with the new value.
        :param key: The name of the list
        :param value: The value to append to the list
        :return: None
        """
        value = make_utf8(value)
        key = make_utf8(key)
        try:
            current_value = self.local_contents[key]
            if current_value is not None and not isinstance(current_value, list):
                self.local_contents[key] = [current_value]

            if self.local_contents[key] is None:
                self.local_contents[key] = []

            if value not in self.local_contents[key]:
                self.local_contents[key].append(value)
            else:
                return False
        except (AttributeError, KeyError):
            self.local_contents[key] = [value]
        self.changed = True
        return True

    def remove(self, key, value):
        """
        Remove a value from a list specified by key.
        If the current value specified by the key is not a list, unset the value.
        :param key: The name of the list parameter to manipulate
        :param value: The value to attempt to remove
        :return: True if the value was in the list, False if it was not
        """
        value = make_utf8(value)
        key = make_utf8(key)
        try:
            current_value = self.local_contents[key]
            if current_value is not None and not isinstance(current_value, list) and current_value == value:
                return self.unset(key)

            if value in current_value:
                self.local_contents[key].remove(value)
            else:
                return False
            self.changed = True
            return True
        except (AttributeError, KeyError, ValueError):
            return False

    def unset(self, key):
        """
        Unsets a key
        :param key: The key to unset
        :return: boolean
        """
        key = make_utf8(key)

        # Special handling is required for the SLA, since it deviates from the typical CP
        # empty => null semantics
        if key == 'service_level_agreement':
            value = self.local_contents.get(key, None)
            self.local_contents[key] = ''
        else:
            value = self.local_contents.pop(key, None)

        if value is not None:
            self.changed = True

        return value is not None

    def set(self, key, value):
        """
        Set a key (syspurpose parameter) to value
        :param key: The parameter of the syspurpose file to set
        :type key: str

        :param value: The value to set that parameter to
        :return: Whether any change was made
        """
        value = make_utf8(value)
        key = make_utf8(key)
        org = make_utf8(self.local_contents.get(key, None))
        self.local_contents[key] = value

        if org != value or org is None:
            self.changed = True

        return org != value or org is None


class JsonSyncedStore(SyncedStore):

    def get_local_contents(self):
        self.local_file = open(self.path, 'w', encoding='utf-8')
        return json.load(self.local_file)

    def get_cached_contents(self):
        self.cache_file = open(self.path, 'w', encoding='utf-8')
        return json.load(self.cache_file)


class UserSyspurposeStore(JsonSyncedStore):

    PATH = USER_SYSPURPOSE
    CACHE_PATH = CACHED_SYSPURPOSE

    def __init__(self, uep, on_changed=None, consumer_uuid=None, report=None):
        super(UserSyspurposeStore, self).__init__(uep, on_changed=on_changed)
        self.consumer_uuid = consumer_uuid
        self.report = report

    def sync(self):
        result = super(UserSyspurposeStore, self).sync()

        if self.report is not None:
            self.report._status = 'Successfully synced system purpose'

        return result

    def get_remote_contents(self):
        consumer = self.uep.getConsumer(self.consumer_uuid)
        result = {}

        # Translate from the remote values to the local, filtering out items not known
        for attr in ATTRIBUTES:
            value = consumer.get(LOCAL_TO_REMOTE[attr])
            if value is not None:
                result[attr] = value

        return result

    def update_remote(self, data):
        addons = data.get(ADDONS)
        self.uep.updateConsumer(
                self.consumer_uuid,
                role=data.get(ROLE) or "",
                addons=addons if addons is not None else [],
                service_level=data.get(SERVICE_LEVEL) or "",
                usage=data.get(USAGE) or ""
        )


def read_syspurpose(raise_on_error=False):
    """
    Reads the system purpose from the correct location on the file system.
    Makes an attempt to use a SyspurposeStore if available falls back to reading the json directly.
    :return: A dictionary containing the total syspurpose.
    """
    if SyspurposeStore is not None:
        try:
            syspurpose = SyspurposeStore.read(USER_SYSPURPOSE).contents
        except (OSError, IOError):
            syspurpose = {}
    else:
        try:
            syspurpose = json.load(open(USER_SYSPURPOSE))
        except (os.error, ValueError, IOError):
            # In the event this file could not be read treat it as empty
            if raise_on_error:
                raise
            syspurpose = {}
    return syspurpose