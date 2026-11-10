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

import os

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Base URL of your Cloudera Manager API, for example
# https://<cm-host>:7183/api/v54
CM_BASE = os.environ.get("CM_BASE", "")


def create_session() -> requests.Session:
    """Create a Cloudera Manager API session that uses basic authentication.

    The session reads the user name and the password from the environment.
    Set CM_USER and CM_PASSWORD. If CM_USER is empty, the session uses the
    USER value of the operating system.
    """
    user = os.environ.get("CM_USER") or os.environ.get("USER", "")
    password = os.environ.get("CM_PASSWORD", "")

    session = requests.Session()
    session.auth = (user, password)
    session.verify = False
    session.headers.update({"accept": "application/json"})
    return session
