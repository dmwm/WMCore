import re

# path to static resources
RX_STATIC_DIR_PATH = re.compile(r"^([a-zA-Z]+/)+[-a-z0-9_]+\.(?:css|js|png|gif|html)$")

# name of the request in the ReqMgr
RX_REQUEST_NAME = re.compile(r"^[-A-Za-z0-9_]+$")

# name of a group in the ReqMgr
RX_GROUP_NAME = re.compile(r"^[-A-Za-z0-9_]+$")

# name of a team in the ReqMgr
RX_TEAM_NAME = re.compile(r"^[-A-Za-z0-9_]+$")

# boolean flag in the URL argument
RX_BOOL_FLAG = re.compile(r"^(true|false)$")
