#!/bin/sh
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "${PROXYCREATE}"
mysql -u root --socket=$TESTDIR/mysqldata/mysql.sock --exec "create database ${PROXYDB}"

