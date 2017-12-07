FROM cerise
MAINTAINER Lourens Veen <l.veen@esciencecenter.nl>

# Install code coverage
RUN pip3 install coverage

# Copy integration test configuration into container
COPY conf/config.yml /home/cerise/conf/config.yml
RUN chown -R cerise:cerise /home/cerise/conf
COPY conf/docker-init.sh /usr/local/bin/init.sh
RUN chmod 755 /usr/local/bin/init.sh

# Copy credentials
RUN mkdir /home/cerise/.ssh
COPY conf/known_hosts /home/cerise/.ssh/
RUN chown -R cerise:cerise /home/cerise/.ssh/known_hosts

# Copy API
COPY api/ /home/cerise/api

