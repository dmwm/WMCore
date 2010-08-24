#!/bin/sh

export DBUSER=hr
export DBPASS=root
export DIALECT=Oracle

echo "-->Creating Oracle database access string"
export DBHOST=localhost
export DATABASE=oracle://${DBUSER}:${DBPASS}@${DBHOST}:1521/XE
echo '-->Using Oracle DB: ' $DATABASE


