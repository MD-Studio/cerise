FROM simple-cwl-xenon-service
MAINTAINER Lourens Veen <l.veen@esciencecenter.nl>

# Copy integration test configuration into container
COPY conf/config.yml /home/simple_cwl_xenon_service/conf/config.yml
RUN chown -R simple_cwl_xenon_service:simple_cwl_xenon_service /home/simple_cwl_xenon_service/conf

RUN mkdir /home/simple_cwl_xenon_service/.ssh
COPY conf/known_hosts /home/simple_cwl_xenon_service/.ssh/
RUN chown -R simple_cwl_xenon_service:simple_cwl_xenon_service /home/simple_cwl_xenon_service/.ssh/known_hosts

