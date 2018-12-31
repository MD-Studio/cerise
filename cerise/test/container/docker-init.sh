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

# Note: systemd will strip the environment, so credentials are
# not passed here, which is what we want.
service nginx start

cd /home/cerise

su -c "coverage run --source=cerise cerise/run_back_end.py >>/var/log/cerise/cerise_backend.err 2>&1" cerise &

su -c "gunicorn --pid ${gunicorn_pid_file} --access-logfile /var/log/gunicorn/access.log --error-logfile /var/log/gunicorn/error.log --capture-output --bind 127.0.0.1:29594 -k gevent --workers 1 cerise.run_front_end:application" cerise &

wait

# Give coverage.py a chance to write output
sleep 1

rm $gunicorn_pid_file
rm $backend_pid_file

