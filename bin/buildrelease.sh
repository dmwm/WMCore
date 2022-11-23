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

echo "Generating CHANGES file"
LASTCOMMITLINE=$(git log -n1 --oneline -E --grep="^[0-9]+\.[0-9]+\.[0-9]+\.*(rc|patch)*[0-9]*$")
LASTCOMMIT=$(echo ${LASTCOMMITLINE} | awk '{print $1}')
LASTVERSION=$(echo ${LASTCOMMITLINE} | awk '{print $2}')

TMP=$(mktemp -t wmcore.${LASTVERSION}.XXXXX)
TMP_INFO=$(mktemp -t info.${LASTVERSION}.XXXXX)
TMP_COMMIT_HASHES=$(mktemp -t hashes.${LASTVERSION}.XXXXX)
TMP_PR=$(mktemp -t pr.${LASTVERSION}.XXXXX)

echo "${LASTVERSION} to ${TAG}:" >> $TMP

# Grab all the commit hashes since the last release
git log --no-merges  --pretty=format:'%H' ${LASTCOMMIT}.. >> $TMP_COMMIT_HASHES
echo "" >> $TMP_COMMIT_HASHES

# Grab all the commit messages and committers since the last release
git log --no-merges  --pretty=format:'  - %s (%aN)' ${LASTCOMMIT}.. >> $TMP_INFO
echo "" >> $TMP_INFO

# Use gitub public API to fetch pull request # from commit hash
cat $TMP_COMMIT_HASHES | while read line; do
  PR=$(curl -s https://api.github.com/search/issues?q=sha:$line | grep -Po '\"html_url\": \"https://github.com/dmwm/WMCore/pull/\K[0-9]+' | sort | uniq)
  # Commits should have an associated PR, but just in case...
  if [[ $PR ]]
    then
    echo "#$PR" >> $TMP_PR
  else
    echo "" >> $TMP_PR
  fi
done

# Paste info and PR files together
paste -d " " $TMP_INFO $TMP_PR >> $TMP

echo "" >> $TMP
echo "" >> $TMP
cat CHANGES >> $TMP
cp $TMP CHANGES
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
