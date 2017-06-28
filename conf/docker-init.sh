#!/bin/bash

backend_pid_file='/home/simple_cwl_xenon_service/run/scxs_backend.pid'
gunicorn_pid_file='/home/simple_cwl_xenon_service/run/gunicorn.pid'

function stop_container {
    service nginx stop

    backend_pid=$(cat $backend_pid_file)
    kill -TERM ${backend_pid}

    gunicorn_pid=$(cat $gunicorn_pid_file)
    kill -TERM ${gunicorn_pid}
}

trap stop_container SIGTERM


service nginx start

cd /home/simple_cwl_xenon_service

su -c "python3 simple_cwl_xenon_service/run_back_end.py" simple_cwl_xenon_service &

su -c "gunicorn --pid ${gunicorn_pid_file} --access-logfile /var/log/gunicorn/access.log --error-logfile /var/log/gunicorn/error.log --capture-output --bind 0.0.0.0:29593 -k gevent --workers 1 simple_cwl_xenon_service.run_front_end:application" simple_cwl_xenon_service &

wait

rm $gunicorn_pid_file
rm $backend_pid_file

