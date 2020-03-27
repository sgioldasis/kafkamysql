from .context import kafkamysql
import pytest
import json
import time
from decimal import Decimal
import datetime


@pytest.fixture(scope="function")
def input_id():
    return "d6707ce40a1447baaf012f948fb5b356"


@pytest.fixture(scope="function")
def input_data(input_id):
    data = {
        "id": input_id,
        "customer_id": "8e0fdcefc3ad45acbb5a2abc506c6c9f",
        "created_at": "2019-05-04T23:34:19.5934998Z",
        "text": "<p>aliquam sed amet dolore consectetuer ut dolore elit dolor euismod elit erat</p>",
        "ad_type": "Premium",
        "price": 29.04,
        "currency": "EUR",
        "payment_type": "Card",
        "payment_cost": 0.17,
    }
    json_str = json.dumps(data)
    return json_str


@pytest.fixture(scope="function")
def output_data(input_id):
    return [
        (
            input_id,
            "8e0fdcefc3ad45acbb5a2abc506c6c9f",
            "2019-05-04T23:34:19.5934998Z",
            "<p>aliquam sed amet dolore consectetuer ut dolore elit dolor euismod elit erat</p>",
            "Premium",
            Decimal("29.04"),
            "EUR",
            "Card",
            Decimal("0.17"),
            datetime.datetime(2019, 5, 4, 23, 34, 19, 593499),
            800,
        )
    ]


# Get record with specific id from database
def db_get_id(db_connection, mysql_table, input_id):
    db_cursor = db_connection.cursor()
    db_cursor.execute(f"select * from `{mysql_table}` where id='{input_id}'")
    result = db_cursor.fetchall()
    db_cursor.close()
    return result


def test_app(
    env,
    kafka_producer,
    kafka_topic,
    input_data,
    db_connection,
    mysql_table,
    input_id,
    output_data,
    capsys,
    caplog,
):

    # Write input data to Kafka topic
    kafka_producer.produce(kafka_topic, key="1", value=input_data)
    kafka_producer.produce(kafka_topic, key="2", value=input_data)
    kafka_producer.produce(kafka_topic, key="3", value="Quit!")
    kafka_producer.flush()

    # pylint: disable=W0612,W0613
    kafkamysql.KafkaMySql.run(env)
    captured = capsys.readouterr()

    # Check output data
    assert db_get_id(db_connection, mysql_table, input_id) == output_data
    assert 'Offending data: \nQuit!' in caplog.text
