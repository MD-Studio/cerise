#!/bin/bash

gunicorn_pid_file='/home/simple_cwl_xenon_service/run/gunicorn.pid'

function stop_container {
	echo 'Shutting down simple-cwl-xenon-service'
	service nginx stop

	gunicorn_pid=$(cat $gunicorn_pid_file)
	kill -TERM ${gunicorn_pid}
}

trap stop_container SIGTERM

service nginx start

cd /home/simple_cwl_xenon_service
su -c "gunicorn --pid ${gunicorn_pid_file} --access-logfile /var/log/gunicorn/access.log --error-logfile /var/log/gunicorn/error.log --capture-output --bind 0.0.0.0:29593 -k gevent --workers 5 simple_cwl_xenon_service.__main__:application" simple_cwl_xenon_service &

wait

