# Copyright 2026 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The name filter is what keeps one campaign cell's logs out of another's.

A date window cannot do it: thirteen cells run back to back all sit inside the
History Server's seven-day retention, so every collect after the first would
pull its predecessors too, and the feature pipeline stamps a single set of
network constants onto every log in a directory.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.spark.downloader import list_app_ids


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class _Session:
    """Records the request and replays one canned listing."""

    def __init__(self, payload):
        self.payload = payload
        self.params = None

    def get(self, url, params=None, timeout=None):
        self.params = params
        return _Resp(self.payload)


APPS = [
    {"id": "app_0011", "name": "dex_tpcds_sf1000-onprem-baseline-20260906T130639"},
    {"id": "app_0012", "name": "dex_tpcds_sf1000-bw10-rtt10-20260907T090000"},
    {"id": "app_0013", "name": "dex_tpcds_sf1000-bw10-rtt25-20260907T130000"},
    {"id": "app_0014", "name": "dex_tpcds_sf1000-onprem-baseline2-20260906T160000"},
    {"id": "app_0007", "name": "TPCDSDataGen"},
    {"id": "app_bad", "name": None},
]


def test_without_a_filter_every_application_in_the_window_comes_back():
    s = _Session(APPS)
    assert list_app_ids(s, "u", None, None) == [
        "app_0011", "app_0012", "app_0013", "app_0014", "app_0007", "app_bad"]


def test_the_filter_keeps_only_the_named_run():
    s = _Session(APPS)
    assert list_app_ids(s, "u", None, None, name_contains="-bw10-rtt10-") == ["app_0012"]


def test_a_label_that_prefixes_another_does_not_take_it_too():
    """`onprem-baseline` is a prefix of `onprem-baseline2`.

    The run name is `<db>-<label>-<timestamp>`, so delimiting the label with the
    dashes that surround it is what separates the two. This is the reason the
    campaign passes `-$(RUN_LABEL)-` rather than the bare label.
    """
    s = _Session(APPS)
    assert list_app_ids(s, "u", None, None, name_contains="-onprem-baseline-") == ["app_0011"]
    assert list_app_ids(s, "u", None, None, name_contains="-onprem-baseline2-") == ["app_0014"]


def test_an_application_with_no_name_is_filtered_out_rather_than_crashing():
    s = _Session(APPS)
    assert "app_bad" not in list_app_ids(s, "u", None, None, name_contains="-bw10-rtt10-")


def test_a_filter_that_matches_nothing_returns_nothing_rather_than_everything():
    s = _Session(APPS)
    assert list_app_ids(s, "u", None, None, name_contains="-bw2-rtt0-") == []
