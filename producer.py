import requests
import os
import json
from kafka import KafkaProducer
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="snowwhite",
  database="twitter"
  )

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic_name = "dbt_project"

bearer_token = "AAAAAAAAAAAAAAAAAAAAADq3cAEAAAAAtnrY6hZUo7ooBZ6e41n%2BN28X33A%3D7c8cLd1ClrRc1hPONeOrIaxYAfTvRNP5YsVyQawPAw9QNKqMSy"


def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "IPLTwitterStreamer"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(delete):
    sample_rules = [
        {"value": "IPL", "tag": "ipl"}
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            json_return = json_response["data"]
            raw_data=json.dumps(json_return, indent=4, sort_keys=True)
            producer.send(topic_name, value=json_response["data"])

            tweet=str(json_return["text"])
            print(tweet)
            mycursor = mydb.cursor()

            sql = "INSERT INTO twitter (tweet) VALUES (%s)"
            val = (tweet)
            mycursor.execute(sql, [tweet])

            mydb.commit()

            print(raw_data)


def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set)


if __name__ == "__main__":
    main()