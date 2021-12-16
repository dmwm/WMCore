FROM registry.cern.ch/cmsweb/cmsweb-base:20211201-stable as cmsweb
FROM debian:stable-slim
MAINTAINER Valentin Kuznetsov vkuznet@gmail.com

ENV WDIR=/data
ENV USER=_dmwm
WORKDIR $WDIR

# tag to use
RUN apt-get update && \
    apt-get install -y curl git python3 python3-distutils python3-pip libcurl4-openssl-dev libssl-dev \
    libjemalloc-dev apache2-utils sudo
RUN git clone https://github.com/dmwm/WMCore.git
WORKDIR $WDIR/WMCore
# ENV WTAG=`grep version src/python/WMCore/__init__.py | awk '{print $3}' | sed -e "s,',,g"`
ENV WTAG=1.5.6
RUN git checkout tags/$WTAG -b build
RUN sed -i -e "s,==,>=,g" requirements_py3.txt
RUN python3 -m pip install -r requirements_py3.txt
RUN find . -type f | awk '{print "sed -i -e \"s,/usr/bin/env python,/usr/bin/env python3,g\" "$0""}' | /bin/sh
RUN python3 setup.py install_system -s reqmgr2ms --prefix=/data/install
ENV PYTHONPATH=$WDIR/WMCore/src/python:/etc/secrets
COPY --from=cmsweb /etc/grid-security/certificates /etc/grid-security/certificates
