#!/bin/bash
# the iam-token.sh scripts generate new IAM token
# it relies on the following environment variables
# IAM_CLIENT_ID client id value obtained from IAM provider
# IAM_CLIENT_SECRET client secret value obtained from IAM provider
# IAM_TOKEN output file name to store obtained IAM token
# All steps to obtain client credentials can be found:
# https://github.com/dmwm/WMCore/pull/11093#issuecomment-1098131010

# check if curl exist on a system
if ! command -v curl &> /dev/null
then
    echo "curl could not be found, please install it on your system"
    exit
fi
# check if jq exist on a system
if ! command -v jq &> /dev/null
then
    echo "jq could not be found, please install it on your system"
    exit
fi

#echo "tools are checked"

# use either IAM_CLIENT_ID, /etc/secrets/client_id or fail
if [ -n "$IAM_CLIENT_ID" ] && [ -f $IAM_CLIENT_ID ]; then
    export client_id=`cat $IAM_CLIENT_ID`
else
    echo "unable to locate client_id file, please either setup IAM_CLIENT_ID to point to your client_id file name"
    exit
fi
#echo "use client_id=$client_id"

# use either IAM_CLIENT_SECRET, /etc/secrets/client_secret or fail
if [ -n "$IAM_CLIENT_SECRET" ] && [ -f $IAM_CLIENT_SECRET ]; then
    export client_secret=`cat $IAM_CLIENT_SECRET`
else
    echo "unable to locate client_secret file, please either setup IAM_CLIENT_secret to point to your client_secret file name"
    exit
fi
#echo "use client_secret=$client_secret"

# obtain new token using client credentials
if [ -n "IAM_TOKEN" ]; then
    # grant_type=client_credentials key=value pair is required by IAM provider
    # to specify that request contains clients credentials
    curl -s -k -d grant_type=client_credentials \
            -u ${client_id}:${client_secret} \
            https://cms-auth.web.cern.ch/token | jq -r '.access_token' > $IAM_TOKEN
    echo "New IAM token generated and can be found at $IAM_TOKEN"
else
    echo "Please setup IAM_TOKEN environment variable pointing to a file name where token will be written"
    exit
fi
