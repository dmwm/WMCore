import re

# path to static resources
RX_STATIC_DIR_PATH = re.compile(r"^([a-z]+/)+[-a-z0-9_]+\.(?:css|js|png|gif|html)$")

# name of the request in the ReqMgr
RX_REQUEST_NAME = re.compile(r"^[-A-Za-z0-9_]+$")

# boolean flag in the URL argument
RX_BOOL_FLAG = re.compile(r"^(true|false)$")