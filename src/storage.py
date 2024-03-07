#!/usr/bin/env python3

# Copyright 2024 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Handle Juju Storage Events."""

import json
import logging
from subprocess import CalledProcessError, run

import ops_sunbeam.guard as sunbeam_guard
from ops.charm import CharmBase, StorageAttachedEvent, StorageDetachingEvent
from ops.framework import Object, StoredState
from ops.model import ActiveStatus, MaintenanceStatus
from tenacity import retry, stop_after_attempt, wait_fixed

import microceph

logger = logging.getLogger(__name__)


class StorageHandler(Object):
    """The Storage class manages the Juju storage events.

    Observes the following events:
    1) *_storage_attached
    2) *_storage_detaching
    """

    name = "storage"

    # storage directive names
    standalone = "osd-standalone"

    charm = None
    # _stored: per unit stored state for storage class. Contains:
    #  osd_data: dict of dicts with int (osd num) key
    #    disk_by_id: OSD disk by id (unique)
    #    disk: OSD disk storage name (unique)
    _stored = StoredState()

    def __init__(self, charm: CharmBase, name="storage"):
        super().__init__(charm, name)
        self._stored.set_default(osd_data={})
        self.charm = charm
        self.name = name

        # Attach handlers
        self.framework.observe(
            charm.on[self.standalone.replace("-", "_")].storage_attached,
            self._on_osd_standalone_attached,
        )

        # OSD Detaching handlers.
        self.framework.observe(
            charm.on[self.standalone.replace("-", "_")].storage_detaching,
            self._on_storage_detaching,
        )

    # storage event handlers

    def _on_osd_standalone_attached(self, event: StorageAttachedEvent):
        """Storage attached handler for osd-standalone."""
        if not self.charm.ready_for_service():
            logger.warning("MicroCeph not ready yet, deferring storage event.")
            event.defer()
            return

        self._clean_stale_osd_data()

        enroll = []
        for storage in self._fetch_filtered_storages([self.standalone]):
            if not self._get_osd_id(name=storage):
                enroll.append(storage)

        with sunbeam_guard.guard(self.charm, self.name):
            self.charm.status.set(MaintenanceStatus("Enrolling OSDs"))
            self._enroll_disks_in_batch(enroll)
            self.charm.status.set(ActiveStatus("charm is ready"))

    def _on_storage_detaching(self, event: StorageDetachingEvent):
        """Unified storage detaching handler."""
        # check if the detaching device (of the form directive/index)
        # is being used as or with an OSD.
        osd_num = self._get_osd_id(event.storage.full_id)

        if osd_num is not None:
            return

        with sunbeam_guard.guard(self.charm, self.name):
            try:
                self.remove_osd(osd_num)
            except CalledProcessError as e:
                if self._is_safety_failure(e.stderr):
                    warning = f"Storage {event.storage.full_id} detached, provide replacement for osd.{osd_num}."
                    logger.warning(warning)
                    # forcefully remove OSD and entry from stored state
                    # because Juju WILL deprovision storage.
                    self.remove_osd(osd_num, force=True)
                    raise sunbeam_guard.BlockedExceptionError(warning)

    # helper functions

    def _fetch_filtered_storages(self, directives: list) -> list:
        """Provides a filtered list of attached storage devices."""
        filtered = []
        for device in self.juju_storage_list():
            if device.split("/")[0] in directives:
                filtered.append(device)

        return filtered

    def _is_safety_failure(self, err: str) -> bool:
        """Checks if the subprocess error is caused by safety check."""
        return "need at least 3 OSDs" in err

    def _run(self, cmd: list) -> str:
        """Wrapper around subprocess run for storage commands."""
        process = run(cmd, capture_output=True, text=True, check=True, timeout=180)
        logger.debug(f"Command {' '.join(cmd)} finished; Output: {process.stdout}")
        return process.stdout

    def _enroll_disks_in_batch(self, disks: list):
        """Adds requested Disks to Microceph and stored state."""
        # Enroll OSDs
        disk_paths = map(
            lambda name: self.juju_storage_get(storage_id=name, attribute="location"), disks
        )
        microceph.enroll_disks_as_osds(disk_paths)

        # Save OSD data using storage names.
        for disk in disks:
            self._save_osd_data(disk)

    def remove_osd(self, osd_num: int, force: bool = False):
        """Removes OSD from MicroCeph and from stored state."""
        try:
            microceph.remove_disk_cmd(osd_num, force)
            # if no errors while removing OSD, clean stale osd records.
            self._clean_stale_osd_data()
        except CalledProcessError as e:
            if force:
                # If forced removal was done, clean stale osd records.
                self._clean_stale_osd_data()
            raise e

    def _save_osd_data(self, disk_name: str, db_name: str = None):
        """Save OSD data using juju storage names."""
        disk_path = self.juju_storage_get(storage_id=disk_name, attribute="location")

        for osd in microceph.list_disk_cmd()["ConfiguredDisks"]:
            # get block device info using /dev/disk-by-id and lsblk.
            local_device = microceph._get_disk_info(osd["path"])

            # OSD not configured on current unit.
            if not local_device:
                continue

            # e.g. check 'vdd' in '/dev/vdd'
            if local_device["name"] in disk_path:
                logger.debug(f"Added OSD {osd['osd']} with Disk {disk_name}, DB {db_name}")
                self._stored.osd_data[osd["osd"]] = {
                    "disk_by_id": osd["path"],  # /dev/disk-by-id/ for OSD device.
                    "disk": disk_name,  # storage name for OSD device.
                    "db": db_name,  # storage name for DB device.
                }

    def _get_osd_id(self, name: str):
        """Fetch the OSD number of consuming OSD, None is not used as OSD."""
        # storage name is of the form db/3 or osd-standalone/2 etc.
        directive = name.split("/")[0]

        if directive == self.standalone:
            directive = "disk"

        logger.debug(self._stored.osd_data)
        logger.debug(f"Searching for disk {name}")

        for k, v in dict(self._stored.osd_data).items():
            # if value is not None.
            if v and v[directive] == name:
                return k  # key is the stored osd number.
        return None

    def _clean_stale_osd_data(self):
        """Compare with disk list and remove stale entries."""
        osds = [osd["osd"] for osd in microceph.list_disk_cmd()["ConfiguredDisks"]]

        for osd_num in dict(self._stored.osd_data).keys():
            if osd_num not in osds:
                val = self._stored.osd_data[osd_num]
                self._stored.osd_data[osd_num] = None
                logger.debug(f"Popped {val}")

    # NOTE(utkarshbhatthere): 'storage-get' sometimes fires before
    # requested information is available.
    @retry(wait=wait_fixed(5), stop=stop_after_attempt(10))
    def juju_storage_get(self, storage_id=None, attribute=None):
        """Get storage attributes."""
        _args = ["storage-get", "--format=json"]
        if storage_id:
            _args.extend(("-s", storage_id))
        if attribute:
            _args.append(attribute)
        try:
            return json.loads(self._run(_args))
        except ValueError as e:
            logger.error(e)
            return None

    def juju_storage_list(self, storage_name=None):
        """List the storage IDs for the unit."""
        _args = ["storage-list", "--format=json"]
        if storage_name:
            _args.append(storage_name)
        try:
            return json.loads(self._run(_args))
        except ValueError as e:
            logger.error(e)
            return None
        except OSError as e:
            import errno

            if e.errno == errno.ENOENT:
                # storage-list does not exist
                return []
            raise
