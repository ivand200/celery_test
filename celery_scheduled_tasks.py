from celery import Celery 
from celery.schedules import crontab 

celery = Celery(broker="redis://:bsIwj0mAE3zODIm3irCJjn4KVAfWDfBp@redis-18506.c299.asia-northeast1-1.gce.cloud.redislabs.com:18506")

logger = celery.log.get_default_logger()

@celery.task
def scheduled_task(timing):
    logger.info(f"Scheduled task executed {timing}")


celery.conf.beat_schedule = {
    # Executes every 15 seconds
    "every-15-seconds": {
        "task": "celery_scheduled_tasks.scheduled_task",
        "schedule": 15,
        "args": ("every 15 seconds",),
    },

    # Executes following crontab
    "every-2-minutes": {
        "task": "celery_scheduled_tasks.scheduled_task",
        "schedule": crontab(minute="*/2"),
        "args": ("crontab every 2 minutes",),
    },
}

# We need to start a specific worker, the beat worker:
# celery -A celery_scheduled_tasks beat
# celery -A celery_scheduled_tasks worker -l INFO -B
# We need to start celery beat, which is a specific 
# worker that inserts the tasks in the queue.

# Good practice to use a periodic tasks as a "heartbeat"
# to check that system is working correctly.