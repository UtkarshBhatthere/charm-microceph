#!/usr/bin/env python3

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


"""CephClientRequirerMock Charm.

This charm deploys ceph client interface to test
microceph charm.
"""

import interface_ceph_client.ceph_client as ceph_client
from ops.charm import CharmBase
from ops.framework import EventBase
from ops.main import main
from ops.model import ActiveStatus


class CephClientRequirerMock(CharmBase):
    def __init__(self, framework) -> None:
        super().__init__(framework)
        self.ceph = ceph_client.CephClientRequires(
            self,
            "ceph",  # relation name
        )
        self.framework.observe(self.ceph.on.broker_available, self.request_pools)
        self.unit.status = ActiveStatus("")

    def request_pools(self, event: EventBase) -> None:
        self.ceph.create_replicated_pool(
            name=f"{self.app.name}-metadata",
            replicas=3,
            weight=40 * 0.01,
            app_name=f"{self.app.name}",
        )


if __name__ == "__main__":
    main(CephClientRequirerMock)
