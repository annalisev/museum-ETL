'''File which consumes lmnh data.'''
from os import environ as ENV
import argparse
import logging
import json
from datetime import datetime, time

from dotenv import load_dotenv
from confluent_kafka import Consumer
from psycopg2 import connect
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection


INCOMPLETE_DATA_MSG = 'INVALID: Incomplete data.'
INVALID_EXHIBITION_MSG = 'INVALID: Invalid exhibition.'
INVALID_VALUE_MSG = 'INVALID: Invalid value.'
INVALID_TYPE_MSG = 'INVALID: Invalid type.'
CANT_CONVERT_MSG = 'INVALID: Time is in the wrong format.'
INVALID_TIME_MSG = 'INVALID: Interaction recorded while museum closed.'


def get_db_connection() -> connection:
    """Returns a connection to the DB."""
    return connect(
        dbname=ENV.get("DB_NAME", "museum"),
        user=ENV.get("DB_USER", "postgres"),
        password=ENV.get("DB_PASSWORD"),
        host=ENV.get("DB_HOST"),
        port=ENV.get("DB_PORT", 5432),
        cursor_factory=RealDictCursor
    )


def set_up_arguments() -> bool:
    '''Sets up functions.'''
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log_to_file",  action="store_true",
                        help="if this flag is present, logging events will be recorded to a file.")
    args = parser.parse_args()
    return args.log_to_file


def convert_time(msg: dict) -> dict:
    '''Function which converts str into datetime within msg'''
    try:
        msg["at"] = datetime.strptime(msg["at"], "%Y-%m-%dT%H:%M:%S.%f%z")
    except:
        return msg, CANT_CONVERT_MSG
    t = msg["at"].time()
    if t < time(8, 45) or t > time(18, 15):
        return msg, INVALID_TIME_MSG
    return msg, None


def check_messages(msg: dict) -> str:
    '''Function which checks data for possible errors it may contain.'''
    if "site" not in msg.keys() or "at" not in msg.keys() or "val" not in msg.keys():
        return INCOMPLETE_DATA_MSG
    if msg["site"] not in ['0', '1', '2', '3', '4', '5']:
        return INVALID_EXHIBITION_MSG
    if msg["val"] not in [-1, 0, 1, 2, 3, 4]:
        return INVALID_VALUE_MSG
    if "type" in msg.keys():
        if msg["val"] == -1 and msg["type"] not in [0, 1]:
            return INVALID_TYPE_MSG
        if msg["type"] in [0, 1] and msg["val"] != -1:
            return INVALID_VALUE_MSG
    return None


def populate_table(msg):
    '''Function which sends the message to the appropriate table in the database.'''
    conn = get_db_connection()
    if msg['val'] == -1:
        with conn.cursor() as curr:
            curr.execute('''INSERT INTO action
                        (exhibition_id, time_occurred, action_type_id)
                        VALUES  (%s, %s,%s);''', (int(msg["site"])+1, msg['at'], msg["type"]+1))
    else:
        with conn.cursor() as curr:
            curr.execute('''INSERT INTO rating
                        (time_given, rating_value_id, exhibition_id)
                        VALUES (%s,%s,%s);''', (msg['at'], msg["val"]+1, int(msg["site"])+1))
    conn.commit()


if __name__ == "__main__":
    log = set_up_arguments()
    if log:
        logging.basicConfig(filename='consumer.txt',
                            encoding='utf-8', level=logging.INFO)

    load_dotenv()

    c = Consumer({
        'bootstrap.servers': ENV["BOOTSTRAP_SERVERS"],
        'security.protocol': ENV["SECURITY_PROTOCOL"],
        'sasl.mechanisms': ENV["SASL_MECHANISM"],
        'sasl.username': ENV["USERNAME"],
        'sasl.password': ENV["PASSWORD"],
        'group.id': ENV["GROUP"],
        'auto.offset.reset': 'latest',
        'client.id': '3'
    })

    c.subscribe(['lmnh'])
    MESSAGE_LIMIT = 0
    while MESSAGE_LIMIT < 100000:
        message = c.poll(1)
        MESSAGE_LIMIT += 1
        if message:
            message = json.loads(message.value().decode("utf-8"))
            error = check_messages(message)
            if error is None:
                message, error = convert_time(message)
            print(message)
            if error is None:
                populate_table(message)
            elif not log:
                print(error)
            elif log:
                logging.warning(message)
                logging.warning(error)
