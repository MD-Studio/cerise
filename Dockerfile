FROM ubuntu:16.04
MAINTAINER Lourens Veen <l.veen@esciencecenter.nl>

# Install requirements
RUN apt-get update -y && apt-get -y dist-upgrade && \
apt-get install -y python3 python3-pip && \
apt-get install -y default-jdk && \
apt-get install -y nginx-core && \
apt-get install -y less && \
apt-get install -y python python-pip && \
pip install cwltool cwlref-runner && \
apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Add service user
RUN useradd simple_cwl_xenon_service
RUN mkdir /home/simple_cwl_xenon_service
RUN chown simple_cwl_xenon_service:simple_cwl_xenon_service /home/simple_cwl_xenon_service

# Make service dir and install dependencies
COPY requirements.txt /home/simple_cwl_xenon_service
RUN cd /home/simple_cwl_xenon_service && pip3 install -r requirements.txt

# Set up WebDAV directories and give service access
RUN mkdir /home/webdav
RUN mkdir /home/webdav/files
RUN mkdir /home/webdav/files/input
RUN mkdir /home/webdav/client_temp
RUN chown -R www-data:www-data /home/webdav

RUN chmod 775 /home/webdav /home/webdav/files /home/webdav/files/input
RUN usermod -a -G www-data simple_cwl_xenon_service

# Set up service run dirs
RUN mkdir /home/simple_cwl_xenon_service/run
#   We make these in advance to avoid race conditions between the gunicorn
#   worker processes; these may otherwise crash trying to make the same
#   directory simultaneously
RUN mkdir /home/simple_cwl_xenon_service/run/jobs
RUN mkdir /home/simple_cwl_xenon_service/run/files
RUN chown -R simple_cwl_xenon_service:simple_cwl_xenon_service /home/simple_cwl_xenon_service/run
RUN mkdir /var/log/scxs && mkdir /var/log/gunicorn
RUN chown simple_cwl_xenon_service:root /var/log/scxs /var/log/gunicorn

# Copy software into container
COPY . /home/simple_cwl_xenon_service/
COPY conf/docker-config.yml /home/simple_cwl_xenon_service/conf/config.yml
RUN chown -R simple_cwl_xenon_service:simple_cwl_xenon_service /home/simple_cwl_xenon_service
RUN rm -f /home/simple_cwl_xenon_service/run/scxs.db

# Copy WebDAV configuration
COPY conf/docker-nginx.conf /etc/nginx/sites-available/default

# Copy init script
COPY conf/docker-init.sh /usr/local/bin/init.sh
RUN chmod 755 /usr/local/bin/init.sh

# Start the service
CMD ["/bin/bash", "/usr/local/bin/init.sh"]

# USER simple_cwl_xenon_service
# WORKDIR /home/simple_cwl_xenon_service
# CMD ["gunicorn", "--pid", "/var/run/gunicorn.pid", "--bind", "0.0.0.0:29593", "-k", "gthread", "--workers", "2", "--threads", "4", "simple_cwl_xenon_service.__main__:application"]
# CMD tail -f /home/simple_cwl_xenon_service/config.yml

