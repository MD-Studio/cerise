FROM nlesc/xenon-slurm
MAINTAINER Lourens Veen <l.veen@esciencecenter.nl>

RUN apt-get update && \
apt-get install -y python-pip python-dev && \
pip install cwlref-runner

