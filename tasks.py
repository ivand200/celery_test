from celery import Celery 
import requests
from collections import defaultdict


celery = Celery("tasks", broker="redis://:bsIwj0mAE3zODIm3irCJjn4KVAfWDfBp@redis-18506.c299.asia-northeast1-1.gce.cloud.redislabs.com:18506")
logger = celery.log.get_default_logger()
BASE_URL = "https://jsonplaceholder.typicode.com"

celery.conf.beat_schedule = {
    "every 30 sec": {
        "task": "tasks.get_people",
        "schedule": 30.0,
        "args": ()
    },
}

def compose_email(remainders):
    # remainders is a list of (user_info, task_info)
    # Retrieve all the titles from each task_info
    titles = [task["title"] for _, task in remainders]

    # obtain the user_info from the first element
    # The user_info is repeated and the same on each element
    user_info, _ = remainders[0]
    email = user_info["email"]
    # Start the task send_email with the proper info
    send_email.delay(email, titles)


@celery.task
def send_email(email, remainders):
    logger.info(f"Send an email to {email}")
    logger.info(f"Reminders {remainders}")


def obtain_user_info(user_id):
    logger.info(f"Retrieving info for user {user_id}")
    response = requests.get(f"{BASE_URL}/users/{user_id}")
    data = response.json()
    logger.info(f"Info for user {user_id} retrieved")
    return data


@celery.task
def obtain_info():
    logger.info("Starting task")
    users = {}
    task_reminders = defaultdict(list)

    # Call the /todos endpoint to retrieve all the tasks
    response = requests.get(f"{BASE_URL}/todos")
    for task in response.json():
        # Skip complited tasks
        if task["completed"] is True:
            continue

        # Retrieve user info. The info is cached to only ask
        # once per user 
        user_id = task["userId"]
        if user_id not in users:
            users[user_id] = obtain_user_info(user_id)

        info = users[user_id]

        # Append the task information to task_remainders, that 
        # aggregates them per user
        task_data = (info, task)
        task_reminders[user_id].append(task_data)

    # The data is ready to process, create an email per
    # per each
    for user_id, reminders in task_reminders.items():
        compose_email(reminders)

    logger.info("End task")








