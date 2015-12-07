#!/bin/bash

for DIR in */.git; do
    REPO_NAME=$(dirname $DIR)
    CLONE_PATH="/tmp/localclone-${REPO_NAME}"
    REMOVE_CHANCE=1
    FETCH_CHANCE=3
    if [[ $(($RANDOM % 100)) -le $REMOVE_CHANCE ]]; then
        rm -rf $CLONE_PATH
    fi
    if [ ! -e $CLONE_PATH ]; then
      REMOTE_REPO=$(git --git-dir=${REPO_NAME}/.git/ remote  -v | grep origin | grep push | awk '{ print $2 }')
      git init $CLONE_PATH
      git --git-dir="${CLONE_PATH}/.git" remote add origin $REMOTE_REPO
      git --git-dir="${CLONE_PATH}/.git" fetch
    fi
    if [[ $(($RANDOM % 100)) -le $FETCH_CHANCE ]]; then
        git --git-dir="${CLONE_PATH}/.git" fetch
    fi
done

