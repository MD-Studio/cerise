FROM cerise
MAINTAINER Lourens Veen <l.veen@esciencecenter.nl>

# Copy integration test configuration into container
COPY conf/config.yml /home/cerise/conf/config.yml
RUN chown -R cerise:cerise /home/cerise/conf

RUN mkdir /home/cerise/.ssh
COPY conf/known_hosts /home/cerise/.ssh/
RUN chown -R cerise:cerise /home/cerise/.ssh/known_hosts

