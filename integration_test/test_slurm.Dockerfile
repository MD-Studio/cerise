FROM xenon-slurm
MAINTAINER Lourens Veen <l.veen@esciencecenter.nl>

RUN apt-get update && \
apt-get install -y --no-install-recommends python-pip python-dev \
    python-setuptools python-wheel build-essential && \
pip install cwlref-runner && \
apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

