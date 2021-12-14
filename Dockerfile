FROM ubuntu
MAINTAINER Valentin Kuznetsov vkuznet@gmail.com

ENV WDIR=/data
WORKDIR $WDIR

# tag to use
ENV WTAG=1.5.5
RUN apt-get update && \
    apt-get install -y curl git python3 python3-distutils python3-pip libcurl4-openssl-dev libssl-dev \
    libjemalloc-dev apache2-utils
RUN git clone https://github.com/dmwm/WMCore.git
WORKDIR /data/WMCore
#RUN python3 setup.py install_system -s reqmgr2ms --prefix=/data/install
RUN git checkout tags/$WTAG -b build
RUN find . -type f | awk '{print "sed -i -e \"s,/usr/bin/env python,/usr/bin/env python3,g\" "$0""}' | /bin/sh
RUN python3 -m pip install -r requirements_py3.txt
RUN python3 setup.py install_system -s reqmgr2ms --prefix=/data/install
