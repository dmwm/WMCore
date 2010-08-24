#!/bin/sh

export DBNAME=wmbs
export DBUSER=some_user
export DBPASS=some_pass
export DIALECT=MySQL
# if you want to remote connect, change the dbhost
#export DBHOST=localhost
export DBHOST=localhost.localdomain
# to ensure your not using the socket, point it to a nonexisting file
#export DBSOCK=$TESTDIR/mysqldata/mysql.sock
export DBSOCK=/this/file/does/not/exist.txt
# if you connect to remote you might need a 'master acount'
export DBMASTERUSER=root
export DBMASTERPASS=

# ADD BELOW OTHER ADDITIONAL DATABASE PARAMETERS YOU MIGHT NEED:

# we need an extra database if we want to use/test the proxy
export PROXYDB=pa_old
export PROXYDATABASE=mysql://${DBUSER}:${DBPASS}@${DBHOST}/${PROXYDB}

export PROXYCREATE="GRANT ALL PRIVILEGES ON ${PROXYDB}.* TO '${DBUSER}'@'$DBHOST' IDENTIFIED BY '${DBPASS}' WITH GRANT OPTION;"


#DO NOT TOUCH FROM HERE !
echo "-->Creating MySQL database access string"
export DATABASE=mysql://${DBUSER}:${DBPASS}@${DBHOST}/${DBNAME}
echo '-->Using mysql DB: ' $DATABASE
export SQLCREATE="GRANT ALL PRIVILEGES ON ${DBNAME}.* TO '${DBUSER}'@'$DBHOST' IDENTIFIED BY '${DBPASS}' WITH GRANT OPTION;"


