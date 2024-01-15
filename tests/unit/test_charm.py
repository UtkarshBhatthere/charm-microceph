# Copyright 2023 Canonical Ltd.
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

"""Tests for Microceph charm."""

from unittest.mock import MagicMock, PropertyMock, patch

import ops_sunbeam.test_utils as test_utils

import charm
import microceph


class _MicroCephCharm(charm.MicroCephCharm):
    """MicroCeph test charm."""

    def __init__(self, framework):
        """Setup event logging."""
        self.seen_events = []
        super().__init__(framework)


class TestCharm(test_utils.CharmTestCase):
    PATCHES = ["subprocess"]

    def setUp(self):
        """Setup MicroCeph Charm tests."""
        super().setUp(charm, self.PATCHES)
        with open("config.yaml", "r") as f:
            config_data = f.read()
        self.harness = test_utils.get_harness(
            _MicroCephCharm, container_calls=self.container_calls, charm_config=config_data
        )
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_all_relations(self):
        """Test all the charms relations."""
        self.harness.set_leader()
        self.harness.update_config({"snap-channel": "1.0/stable"})
        test_utils.add_complete_peer_relation(self.harness)
        self.subprocess.run.assert_any_call(
            ["sudo", "microceph", "cluster", "bootstrap"],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )

    @patch.object(microceph, "subprocess")
    def test_add_osds_action_with_device_id(self, subprocess):
        """Test action add_osds."""
        test_utils.add_complete_peer_relation(self.harness)
        self.harness._charm.peers.interface.state.joined = True

        action_event = MagicMock()
        action_event.params = {"device-id": "/dev/sdb"}
        self.harness.charm._add_osd_action(action_event)

        action_event.set_results.assert_called()
        action_event.fail.assert_not_called()
        subprocess.run.assert_called_with(
            ["sudo", "microceph", "disk", "add", "/dev/sdb"],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )

    @patch.object(microceph, "subprocess")
    def test_add_osds_action_with_loop_spec(self, subprocess):
        """Test action add_osds with loop file spec."""
        test_utils.add_complete_peer_relation(self.harness)
        self.harness._charm.peers.interface.state.joined = True

        action_event = MagicMock()
        action_event.params = {"loop-spec": "4G,3"}
        self.harness.charm._add_osd_action(action_event)

        action_event.set_results.assert_called()
        action_event.fail.assert_not_called()
        subprocess.run.assert_called_with(
            ["sudo", "microceph", "disk", "add", "loop,4G,3"],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )

    def test_add_osds_action_node_not_bootstrapped(self):
        """Test action add_osds when node not bootstrapped."""
        test_utils.add_complete_peer_relation(self.harness)

        action_event = MagicMock()
        action_event.params = {"device-id": "/dev/sdb"}
        self.harness.charm._add_osd_action(action_event)

        action_event.set_results.assert_not_called()
        action_event.fail.assert_called()

    def _create_subprocess_output_mock(self, stdout):
        _mock = MagicMock()
        self.subprocess.run.return_value = _mock
        type(_mock).stdout = PropertyMock(return_value=stdout)
        return _mock

    def _test_list_disks_action(self, microceph_cmd_output, expected_disks):
        """Test action list_disks."""
        test_utils.add_complete_peer_relation(self.harness)
        self.harness._charm.peers.interface.state.joined = True

        action_event = MagicMock()
        self._create_subprocess_output_mock(microceph_cmd_output)

        self.harness.charm._list_disks_action(action_event)
        action_event.set_results.assert_called_with(expected_disks)

    def test_list_disks_action_node_not_bootstrapped(self):
        """Test action list_disks when node not bootstrapped."""
        test_utils.add_complete_peer_relation(self.harness)

        action_event = MagicMock()
        self.harness.charm._list_disks_action(action_event)
        action_event.set_results.assert_not_called()
        action_event.fail.assert_called()

    def test_list_disks_action_no_osds_no_disks(self):
        microceph_cmd_output = """
Disks configured in MicroCeph:
+-----+----------+------+
| OSD | LOCATION | PATH |
+-----+----------+------+

Available unpartitioned disks on this system:
+-------+----------+------+------+
| MODEL | CAPACITY | TYPE | PATH |
+-------+----------+------+------+
        """

        expected_disks = {"osds": [], "unpartitioned-disks": []}
        self._test_list_disks_action(microceph_cmd_output, expected_disks)

    def test_list_disks_action_no_osds_1_disk(self):
        microceph_cmd_output = """
        Disks configured in MicroCeph:
+-----+----------+------+
| OSD | LOCATION | PATH |
+-----+----------+------+

Available unpartitioned disks on this system:
+---------------+----------+------+-----------------------------------------------------+
|     MODEL     | CAPACITY | TYPE |                        PATH                         |
+---------------+----------+------+-----------------------------------------------------+
| QEMU HARDDISK | 1.00GiB  | scsi | /dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1 |
+---------------+----------+------+-----------------------------------------------------+
        """

        expected_disks = {
            "osds": [],
            "unpartitioned-disks": [
                {
                    "model": "QEMU HARDDISK",
                    "size": "1.00GiB",
                    "type": "scsi",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1",
                }
            ],
        }
        self._test_list_disks_action(microceph_cmd_output, expected_disks)

    def test_list_disks_action_1_osd_no_disks(self):
        microceph_cmd_output = """
Disks configured in MicroCeph:
+-----+-------------+-----------------------------------------------------+
| OSD |  LOCATION   |                        PATH                         |
+-----+-------------+-----------------------------------------------------+
| 0   | microceph-1 | /dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1 |
+-----+-------------+-----------------------------------------------------+

Available unpartitioned disks on this system:
+-------+----------+------+------+
| MODEL | CAPACITY | TYPE | PATH |
+-------+----------+------+------+
        """

        expected_disks = {
            "osds": [
                {
                    "osd": "0",
                    "location": "microceph-1",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1",
                }
            ],
            "unpartitioned-disks": [],
        }
        self._test_list_disks_action(microceph_cmd_output, expected_disks)

    def test_list_disks_action_1_osd_1_disk(self):
        microceph_cmd_output = """
Disks configured in MicroCeph:
+-----+-------------+-----------------------------------------------------+
| OSD |  LOCATION   |                        PATH                         |
+-----+-------------+-----------------------------------------------------+
| 0   | microceph-1 | /dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1 |
+-----+-------------+-----------------------------------------------------+

Available unpartitioned disks on this system:
+---------------+----------+------+-----------------------------------------------------+
|     MODEL     | CAPACITY | TYPE |                        PATH                         |
+---------------+----------+------+-----------------------------------------------------+
| QEMU HARDDISK | 1.00GiB  | scsi | /dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--2 |
+---------------+----------+------+-----------------------------------------------------+
        """

        expected_disks = {
            "osds": [
                {
                    "osd": "0",
                    "location": "microceph-1",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--1",
                }
            ],
            "unpartitioned-disks": [
                {
                    "model": "QEMU HARDDISK",
                    "size": "1.00GiB",
                    "type": "scsi",
                    "path": "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_lxd_osd--2",
                }
            ],
        }
        self._test_list_disks_action(microceph_cmd_output, expected_disks)
