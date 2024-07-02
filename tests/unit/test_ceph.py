# Copyright 2024 Canonical Ltd.
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

"""Tests for the ceph module."""
import unittest
from unittest.mock import patch

import ceph


class TestCeph(unittest.TestCase):

    @patch("ceph.os")
    @patch("ceph.socket")
    @patch("ceph.check_output")
    def test_is_quorum(self, check_output, _skt, os):
        check_output.return_value = b'{"state": "peon"}'
        os.path.exists.return_value = True
        self.assertTrue(ceph.is_quorum())
