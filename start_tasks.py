from tasks import obtain_info

obtain_info.delay()

# celery -A tasks worker --loglevel=INFO -c 3
# starts with three workers with the -c 3 parameter.
# if only one worker is involved, the tasks will be run consecutively,
# making it easier to differentiate between tasks.