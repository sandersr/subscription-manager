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
This module contains utilities for syncing system syspurpose with candlepin server
"""

import collections
import os
import logging
from syspurpose.files import read_syspurpose, USER_SYSPURPOSE, SyspurposeStore, CACHED_SYSPURPOSE, \
    ATTRIBUTES, LOCAL_TO_REMOTE, ROLE, ADDONS, USAGE, SERVICE_LEVEL

# We do not want to have hard dependency on rhsm module nor subscription_manager
try:
    import rhsm
    import rhsm.connection
    import rhsm.certificate2
    import rhsm.config
    from subscription_manager.logutil import init_logger
    init_logger()
    log = logging.getLogger(__name__)
except ImportError:
    rhsm = None


class SyspurposeSync(object):
    """
    Sync local, remote and cached system purpose
    """

    def __new__(cls, *args, **kwargs):
        """
        We do not want to create instance of SyspurposeSync, when rhsm module is not available,
        because it would be useless. When rhsm module is not available, then exception is raised.
        :return: Instance of Syspurpose, when rhsm module is available. Otherwise raise exception.
        """
        if rhsm:
            return super(SyspurposeSync, cls).__new__(cls)
        else:
            raise ImportError('Module rhsm is not available')

    def __init__(self, proxy_server=None, proxy_port=None, proxy_user=None, proxy_pass=None):
        """
        Initialization of SyspurposeSync has to have optional arguments with
        proxy settings, because proxy settings can be part of e.g. command line arguments
        """

        self.config = rhsm.config.initConfig()

        if proxy_server:
            self.proxy_server = proxy_server
            self.proxy_port = proxy_port or rhsm.config.DEFAULT_PROXY_PORT
            self.proxy_user = proxy_user
            self.proxy_pass = proxy_pass
        else:
            self.proxy_server = self.config.get('server', 'proxy_hostname')
            self.proxy_port = self.config.get('server', 'proxy_port')
            self.proxy_user = self.config.get('server', 'proxy_user')
            self.proxy_pass = self.config.get('server', 'proxy_password')

        self.connection = None
        self.consumer_uuid = None

    def send_syspurpose_to_candlepin(self):
        """
        Try to sync system purpose to candlepin server.
        :param syspurpose_store: Instance of SystempurposeStore
        :return: True, when system purpose was sent to candlepin server.
        """

        if rhsm:
            cert_dir = self.config.get('rhsm', 'consumerCertDir')
            cert_file_path = cert_dir + '/cert.pem'
            key_file_path = cert_dir + '/key.pem'

            if not os.path.exists(cert_file_path):
                print('Note: System not registered')
                return False

            self.connection = rhsm.connection.UEPConnection(
                proxy_hostname=self.proxy_server,
                proxy_port=self.proxy_port,
                proxy_user=self.proxy_user,
                proxy_password=self.proxy_pass,
                cert_file=cert_file_path,
                key_file=key_file_path
            )

            if not self.connection.has_capability("syspurpose"):
                print("Note: The currently configured entitlement server does not support System Purpose")
                return False

            try:
                consumer_cert = rhsm.certificate.create_from_file(cert_file_path)
            except rhsm.certificate.CertificateException as err:
                print('Unable to read consumer certificate: %s' % err)
                return False
            consumer_uuid = consumer_cert.subject.get('CN')

            try:
                sync(self.connection, consumer_uuid)
            except Exception as err:
                print('Unable to update consumer with system purpose: %s' % err)
                return False

            return True


# A simple container class used to hold the values representing a change detected
# during three_way_merge
DiffChange = collections.namedtuple('DiffChange', ['key', 'previous_value', 'new_value', 'source', 'in_base', 'in_result'])


def three_way_merge(local, base, remote, on_conflict="remote", on_change=None):
    """
    Performs a three-way merge on the local and remote dictionaries with a given base.
    :param local: The dictionary of the current local values
    :param base: The dictionary with the values we've last seen
    :param remote: The dictionary with "their" values
    :param on_conflict: Either "remote" or "local" or None. If "remote", the remote changes
                               will win any conflict. If "local", the local changes will win any
                               conflict. If anything else, an error will be thrown.
    :param on_change: This is an optional function which will be given each change as it is
                      detected.
    :return: The dictionary of values as merged between the three provided dictionaries.
    """
    result = {}
    local = local or {}
    base = base or {}
    remote = remote or {}

    if on_conflict == "remote":
        winner = remote
    elif on_conflict == "local":
        winner = local
    else:
        raise ValueError('keyword argument "on_conflict" must be either "remote" or "local"')

    if on_change is None:
        on_change = lambda change: change

    all_keys = set(local.keys()) | set(base.keys()) | set(remote.keys())

    for key in all_keys:

        local_changed = detect_changed(base=base, other=local, key=key)
        remote_changed = detect_changed(base=base, other=remote, key=key)
        changed = local_changed or remote_changed
        source = 'base'

        if local_changed == remote_changed:
            source = on_conflict
            if key in winner:
                result[key] = winner[key]
        elif remote_changed:
            source = 'remote'
            if key in remote:
                result[key] = remote[key]
        elif local_changed:
            source = 'local'
            if key in local:
                result[key] = local[key]

        if changed:
            original = base.get(key)
            diff = DiffChange(key=key, source=source, previous_value=original,
                              new_value=result.get(key), in_base=key in base,
                              in_result=key in result)
            on_change(diff)

    return result


def detect_changed(base, other, key):
    """
    Detect the type of change that has occurred between base and other for a given key.
    :param base: The dictionary of values we are starting with
    :param other: The dictionary of now current values
    :param key: The key that we are interested in knowing how it changed
    :return: True if there was a change, false if there was no change
    :rtype: bool
    """
    base = base or {}
    other = other or {}

    if key not in other.keys():
        return False

    base_val = base.get(key)
    other_val = other.get(key)

    return base_val != other_val


def sync(uep, consumer_uuid, command=None, report=None):
    """
    Actually do the sync between client and server.
    Saves the merged changes between client and server in the SyspurposeCache.
    :return: The synced values
    """
    if not uep.has_capability('syspurpose') and command != 'service_level_agreement':
        log.debug('Server does not support syspurpose, not syncing')
        return read_syspurpose()

    consumer = uep.getConsumer(consumer_uuid)

    local_sp = read_syspurpose()
    server_sp = {}
    sp_cache = SyspurposeStore.read(CACHED_SYSPURPOSE)

    # Translate from the remote values to the local, filtering out items not known
    for attr in ATTRIBUTES:
        value = consumer.get(LOCAL_TO_REMOTE[attr])
        if value is not None:
            server_sp[attr] = value

    try:
        filesystem_sp = read_syspurpose(raise_on_error=True)
    except (os.error, ValueError):
        if report is not None:
            report._exceptions.append(
                'Cannot read local syspurpose, trying to update from server only'
            )
        result = server_sp
        log.debug('Unable to read local system purpose at  \'%s\'\nUsing the server values.'
                  % USER_SYSPURPOSE)
    else:
        cached_values = sp_cache.contents
        if report is not None:
            result = three_way_merge(local=filesystem_sp, base=cached_values, remote=server_sp,
                                     on_change=report.record_change)
        else:
            result = three_way_merge(local=filesystem_sp, base=cached_values, remote=server_sp)

    sp_cache.contents = result
    sp_cache.write()

    local_sp.contents = result
    local_sp.write()

    addons = result.get(ADDONS)
    uep.updateConsumer(
            consumer_uuid,
            role=result.get(ROLE) or "",
            addons=addons if addons is not None else [],
            service_level=result.get(SERVICE_LEVEL) or "",
            usage=result.get(USAGE) or ""
    )

    if report is not None:
        report._status = 'Successfully synced system purpose'

    log.debug('Updated syspurpose located at \'%s\'' % USER_SYSPURPOSE)

    return result
