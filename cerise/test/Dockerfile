FROM cerise:latest

# Install code coverage
RUN pip3 install coverage

# Copy integration test configuration into container
COPY container/config.yml /home/cerise/conf/config.yml
RUN chown -R cerise:cerise /home/cerise/conf
COPY container/docker-init.sh /usr/local/bin/init.sh
RUN chmod 755 /usr/local/bin/init.sh
COPY container/.coveragerc /home/cerise/.coveragerc

# Copy credentials
RUN mkdir /home/cerise/.ssh
COPY container/known_hosts /home/cerise/.ssh/
RUN chown -R cerise:cerise /home/cerise/.ssh/known_hosts

# Copy API
COPY api /home/cerise/api

