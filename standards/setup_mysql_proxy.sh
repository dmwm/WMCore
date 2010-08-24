#!/bin/sh

# we need an extra database if we want to use/test the proxy
export PROXYDB=pa_old
export PROXYDATABASE=mysql://${DBUSER}:${DBPASS}@${DBHOST}/${PROXYDB}

export PROXYCREATE="GRANT ALL PRIVILEGES ON ${PROXYDB}.* TO '${DBUSER}'@'localhost' IDENTIFIED BY '${DBPASS}' WITH GRANT OPTION;"

