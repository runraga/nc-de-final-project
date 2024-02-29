#!/bin/bash

: '
For Makefile use
generates credentials into $1
parses some psql results for PGUSER
parses ~/.pgpass for PGPASSWORD
'


USERS=($(echo "select current_user;" | psql))
USER=${USERS[2]}

PGPASS=$(cat ~/.pgpass)
PASS=${PGPASS##*:}

echo -e "[DEFAULT]\nPGUSER=$USER\nPGPASSWORD=$PASS\n" > $1
echo "Parsed credentials into $1"