#!/bin/bash

backend_pid_file='/home/cerise/run/cerise_backend.pid'
gunicorn_pid_file='/home/cerise/run/gunicorn.pid'

function stop_container {
    service nginx stop

    backend_pid=$(cat $backend_pid_file)
    kill -TERM ${backend_pid}

    gunicorn_pid=$(cat $gunicorn_pid_file)
    kill -TERM ${gunicorn_pid}
}

trap stop_container SIGTERM


service nginx start

cd /home/cerise

su -c "python3 cerise/run_back_end.py" cerise &

su -c "gunicorn --pid ${gunicorn_pid_file} --access-logfile /var/log/gunicorn/access.log --error-logfile /var/log/gunicorn/error.log --capture-output --bind 0.0.0.0:29593 -k gevent --workers 1 cerise.run_front_end:application" cerise &

wait

rm $gunicorn_pid_file
rm $backend_pid_file

