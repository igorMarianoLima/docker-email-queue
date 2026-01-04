import redis
import json
import os
from time import sleep
from random import randint

if __name__ == '__main__':
    redis_host = os.getenv('REDIS_HOST', 'queue')
    r = redis.Redis(host=redis_host, port=6379, db=0)

    print('Waiting e-mails...')

    while True:
        message = json.loads(r.blpop('sender')[1])

        # Simulate email send
        print('Sending e-mail:', message['subject'])
        sleep(randint(15, 45))
        print('E-mail', message['subject'], 'sended')
