import psycopg2
import redis
import json
import time
import os
from bottle import Bottle, request

class Sender(Bottle):
    def connect_with_retry(self, dsn, retries=10, delay=2):
        for i in range(retries):
            try:
                return psycopg2.connect(dsn)
            except psycopg2.OperationalError as e:
                print(f"DB não pronto, tentativa {i+1}/{retries}")
                time.sleep(delay)
        raise Exception("Banco não ficou disponível")

    def __init__(self):
        super().__init__()
        self.route('/', method='POST', callback=self.send)

        redis_host = os.getenv('REDIS_HOST', 'queue')

        self.queue = redis.StrictRedis(host=redis_host, port=6379, db=0)

        db_host = os.getenv('DB_HOST', 'db')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'postgres')
        db_name = os.getenv('DB_NAME', 'sender')

        DSN = f'dbname={db_name} user={db_user} password={db_password} host={db_host}'
        self.conn = self.connect_with_retry(DSN)

    def register_message(self, subject, message):
        SQL = 'INSERT INTO emails (subject, message) VALUES (%s, %s)'

        cur = self.conn.cursor()
        cur.execute(SQL, (subject, message))
        self.conn.commit()
        cur.close()

        msg = {'subject': subject, 'message': message}
        self.queue.rpush('sender', json.dumps(msg))

        print('Registered email!')

    def send(self):
        subject = request.forms.get('subject')
        message = request.forms.get('message')

        self.register_message(subject, message)

        return 'Queued message! Subject: {}. Message: {}'.format(
            subject,
            message
        )

if __name__ == '__main__':
    sender = Sender()
    sender.run(host='0.0.0.0', port=8080, debug=True)