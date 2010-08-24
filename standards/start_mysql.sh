#!/bin/sh

echo 'This is a local script to start the server!'

echo "-->Starting MySQL server"
# NOTE: do this only if there is no mysql database
mkdir -p $TESTDIR/mysqldata
mysql_install_db --datadir=$TESTDIR/mysqldata
#mysqld_safe --datadir=$TESTDIR/mysqldata --socket=$TESTDIR/mysqldata/mysql.sock --skip-networking --log-error=$TESTDIR/mysqldata/error.log --pid-file=$TESTDIR/mysqldata/mysqld.pid &
#mysqld_safe --defaults-file=$WMCOREBASE/standards/my.cnf --datadir=$TESTDIR/mysqldata --socket=$TESTDIR/mysqldata/mysql.sock --log-error=$TESTDIR/mysqldata/error.log --pid-file=$TESTDIR/mysqldata/mysqld.pid --port=3306 &
mysqld_safe --datadir=$TESTDIR/mysqldata --socket=$TESTDIR/mysqldata/mysql.sock --log-error=$TESTDIR/mysqldata/error.log --pid-file=$TESTDIR/mysqldata/mysqld.pid --port=3306 &
echo 'sleeping to make sure the db exists'
sleep 10

echo "-->Granting access to user for testing"
# granting access
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "${SQLCREATE}"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "create database ${DBNAME}"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "${GRANTSUPER}"

# ADD BELOW OTHER DATABASES IF NEEDED FOR MYSQL BACKEND TESTS.
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "${PROXYCREATE}"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "create database ${PROXYDB}"

