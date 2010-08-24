#!/bin/sh

export DBNAME=wmbs
export DBUSER=some_user
export DBPASS=some_pass
export DIALECT=MySQL

# ADD BELOW OTHER ADDITIONAL DATABASE PARAMETERS YOU MIGHT NEED:

# we need an extra database if we want to use/test the proxy
export PROXYDB=pa_old
export PROXYDATABASE=mysql://${DBUSER}:${DBPASS}@${DBHOST}/${PROXYDB}

export PROXYCREATE="GRANT ALL PRIVILEGES ON ${PROXYDB}.* TO '${DBUSER}'@'localhost' IDENTIFIED BY '${DBPASS}' WITH GRANT OPTION;"


#DO NOT TOUCH FROM HERE !
echo "-->Creating MySQL database access string"
export DBSOCK=$TESTDIR/mysqldata/mysql.sock
export DBHOST=localhost
export DATABASE=mysql://${DBUSER}:${DBPASS}@${DBHOST}/${DBNAME}
echo '-->Using mysql DB: ' $DATABASE
export SQLCREATE="GRANT ALL PRIVILEGES ON ${DBNAME}.* TO '${DBUSER}'@'localhost' IDENTIFIED BY '${DBPASS}' WITH GRANT OPTION;"


