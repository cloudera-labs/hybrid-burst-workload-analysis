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
