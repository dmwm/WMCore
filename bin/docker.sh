#!/bin/bash
# Author: Valentin Kuznetsov <vkuznet@gmail.com>
# helper script to handle docker actions

if [ $# -ne 4 ]; then
     echo "Usage: docker.sh <action> <service> <tag> <registry> "
     echo "Supported actions: build, push"
     exit 1;
fi

# helper function to match the pattern X.Y.Z where X, Y, and Z are numbers
function match_tag {
  local tag=$1
  if [[ $tag =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    return 0
  elif [[ $tag =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    return 0
  else
    return 1
  fi
}

# make input variable assignments
action=$1
service=$2
tag=$3
registry=$4
rurl=$registry/cmsweb/$service

# check if action is supported
case "$action" in
  "build"|"push")
    ;;
  *)
    echo "action: '$action' is not supported"
    exit 1
    ;;
esac

# determine suffix of image tag
suffix=""
if match_tag "$tag"; then
    suffix="-stable"
fi

# check build/push actions and build the image(s) if it will be required
if [ "$action" == "build" ]; then
    echo "action: docker build --build-arg TAG=${tag} --tag ${rurl}:${tag}"
    docker build --build-arg TAG=${tag} --tag ${rurl}:${tag} .
    docker images | grep $tag | grep $service

    if [ -z "$suffix" ]; then
        echo "Building stable image for tag=$tag is not appropriate as tag is not matched X.Y.Z or X.Y.Z.P pattern"
        exit 0
    fi

    echo "action: docker build --build-arg TAG=${tag} --tag ${rurl}:${tag}${suffix}"
    docker build --build-arg TAG=${tag} --tag ${rurl}:${tag}${suffix} .
    docker images | grep ${tag}${suffix} | grep $service
fi

if [ "$action" == "push" ]; then
    image_exist=$(docker images | grep $tag | grep $service | grep -v ${tag}-stable)
    echo $image_exist
    
    if [ -z "$image_exist" ]; then
        echo "Images ${rurl}:${tag} not found"
        exit 1
    fi
    echo "action: docker push ${rurl}:${tag}"
    docker push ${rurl}:${tag}

    # now push the stable release
    if [ -n "$suffix" ]; then
        image_exist_stable=$(docker images | grep ${tag}${suffix} | grep $service)
        echo $image_exist_stable
        if [ -z "$image_exist_stable" ]; then
            echo "Images ${rurl}:${tag}${suffix} not found"
            exit 2
        fi
        echo "action: docker push ${rurl}:${tag}${suffix}"
        docker push ${rurl}:${tag}${suffix}
    fi
fi
