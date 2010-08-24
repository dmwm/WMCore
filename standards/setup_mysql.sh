#!/bin/sh

export DBNAME=wmbs
export DBUSER=some_user
export DBPASS=some_pass
export DIALECT=MySQL

#DO NOT TOUCH FROM HERE !
echo "-->Creating MySQL database access string"
export DBSOCK=$TESTDIR/mysqldata/mysql.sock
export DBHOST=localhost
export DATABASE=mysql://${DBUSER}:${DBPASS}@${DBHOST}/${DBNAME}
echo '-->Using mysql DB: ' $DATABASE
export SQLCREATE="GRANT ALL PRIVILEGES ON ${DBNAME}.* TO '${DBUSER}'@'localhost' IDENTIFIED BY '${DBPASS}' WITH GRANT OPTION;"


