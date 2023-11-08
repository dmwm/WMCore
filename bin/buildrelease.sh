#!/bin/sh
##H
##H Tag & build a WMCore/WMAgent release
##H buildrelease.sh [options]
##H
##H options:
##H -r <remote>       Remote repository to update, default to origin.
##H -g <gitbranch>    Git branch to be tagged, default to master.
##H -t <tag>          Tag name to be created.
##H -h                Show this helper.
##H

REMOTE='origin'
GITBRANCH='master'
TAG=

set -e
for arg; do
  case $arg in
    -h) perl -ne '/^##H/ && do { s/^##H ?//; print }' < $0 1>&2; exit 1 ;;
    -r) REMOTE=$2; shift; shift ;;
    -g) GITBRANCH=$2; shift; shift ;;
    -t) TAG=$2; shift; shift ;;
    -*) echo "$0: unrecognised option $1, use -h for help" 1>&2; exit 2 ;;
  esac
done

if [ -z $TAG ]; then
  perl -ne '/^##H/ && do { s/^##H ?//; print }' < $0 1>&2
  exit 3
fi

# Check whether this git branch is allowed to be tagged
if ! echo ${GITBRANCH} | egrep -iq 'master|_wmagent|_crab|_cmsweb' ; then
  echo "ABORTING - Can only release from master / _crab / _wmagent / _cmsweb branches: ${GITBRANCH}"
  exit 4
fi

# Check whether we are in the correct directory
if [ X$(git rev-parse --show-toplevel) != X$PWD ]; then
  echo "ABORTING - not in root directory $(git rev-parse --show-toplevel)"
  exit 5
fi

# Check if tag exists
if git show-ref --tags --quiet -- $TAG; then
  echo "Tag $TAG exists, skipping tag command"
  exit 6
fi

echo "Checking out branch: $GITBRANCH"
git checkout $GITBRANCH

echo "Building new release of WMCore $TAG on ${GITBRANCH}"
echo "Updating version string ..."
perl -p -i -e "s{__version__ =.*}{__version__ = '$TAG'}g" src/python/WMCore/__init__.py

# fetch the last tag's commit line (hash id plus tag name)
LASTCOMMITLINE=$(git log -n1 --oneline -E --grep="^[0-9]+\.[0-9]+\.[0-9]+\.*(rc|patch)*[0-9]*$")
LASTCOMMIT=$(echo ${LASTCOMMITLINE} | awk '{print $1}')
LASTVERSION=$(echo ${LASTCOMMITLINE} | awk '{print $2}')

echo "Generating CHANGES file"
TMP_CHANGES=$(mktemp -t wmcore.${LASTVERSION}.XXXXX)
echo "${LASTVERSION} to ${TAG}:" >> $TMP_CHANGES

# Grab all the commit hashes, subject and author since the last release
TMP_HASHES_SUBJ_AUTHOR=$(mktemp -t wmcore_hashes.${LASTVERSION}.XXXXX)
git log --no-merges  --pretty=format:'%H %s (%aN)' ${LASTCOMMIT}.. >> $TMP_HASHES_SUBJ_AUTHOR
echo "" >> $TMP_HASHES_SUBJ_AUTHOR

# Use github public API to fetch pull request # from commit hash

cat $TMP_HASHES_SUBJ_AUTHOR | while read commitline; do
  if [ -z "$commitline" ]
  then
      continue  # line is empty
  fi
  HASH_ID=$(echo $commitline | awk '{print $1}')
  PR=$(curl -s https://api.github.com/repos/dmwm/WMCore/commits/$HASH_ID/pulls | grep -Po '\"html_url\": \"https://github.com/dmwm/WMCore/pull/\K[0-9]+' | sort | uniq)
  # remove hash_id from the commit line
  commitline=$(echo $commitline | sed "s+$HASH_ID+  -+")
  echo "$commitline #$PR" >> $TMP_CHANGES
done
echo -en '\n\n' >> $TMP_CHANGES

# append the original CHANGES content and swap the files
cat CHANGES >> $TMP_CHANGES
cp $TMP_CHANGES CHANGES
${EDITOR:-vi} CHANGES
if [ $? -ne 0 ]; then
    echo "User cancelled CHANGES update"
    exit 7
fi

echo "committing local changes ..."
git commit -a -s -m "$TAG"

echo "tagging release ..."
git log --pretty=format:'  - %s' ${LASTCOMMIT}.. | git tag -a $TAG -F -

echo "pushing to ${REMOTE} ..."
git push --tags ${REMOTE} ${GITBRANCH}
set +e

echo "$TAG tagged"

echo
exit 0
