FROM ubuntu:16.04
MAINTAINER Lourens Veen <l.veen@esciencecenter.nl>

# Install requirements
RUN apt-get update -y && apt-get -y dist-upgrade
RUN apt-get install -y python3 python3-pip
RUN apt-get install -y default-jdk

# Add user
RUN useradd simple_cwl_xenon_service

# Copy software into container
COPY . /home/simple_cwl_xenon_service/
COPY config-docker.yml /home/simple_cwl_xenon_service/config.yml
RUN chown -R simple_cwl_xenon_service:simple_cwl_xenon_service /home/simple_cwl_xenon_service

# Install dependencies
RUN cd /home/simple_cwl_xenon_service && pip3 install -r requirements.txt

# Change user, set up and run
USER simple_cwl_xenon_service
RUN mkdir /tmp/simple_cwl_xenon_service
RUN mkdir /tmp/simple_cwl_xenon_service_files
WORKDIR /home/simple_cwl_xenon_service
CMD ["gunicorn", "--bind", "0.0.0.0:29593", "-k", "gthread", "--workers", "2", "--threads", "4", "simple_cwl_xenon_service.__main__:application"]
# CMD tail -f /home/simple_cwl_xenon_service/config.yml

