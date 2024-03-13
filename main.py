import boto3
import os
from dotenv import load_dotenv
import datetime
import json


def query_params() -> dict:
    query_params = {}
    query_params["startTime"] = int(
        (datetime.datetime.now() - datetime.timedelta(minutes=1)).timestamp()
    )
    query_params["endTime"] = int(
        (datetime.datetime.now() - datetime.timedelta(minutes=0)).timestamp()
    )
    query_params["limit"] = 1000

    # query_params["logGroupNames"] = [""]

    query_params["logGroupIdentifiers"] = [""]

    query_params["queryString"] = (
        "fields @timestamp, @message, @logStream, @log, @requestid  | sort @timestamp desc  | limit 1000"
    )

    return query_params


def send_insight(client):
    params = query_params()
    print(params)
    resp = client.start_query(**params)
    return resp


def get_insight_data(client, query_id_dict: dict):
    resp = client.get_query_results(queryId=query_id_dict["queryId"])

    print(f"\n{resp}\n")

    if resp["status"] in ["Running", "Queued"]:
        print(f"Query {query_id_dict['queryId']} is in {resp['status']} state")
        resp = get_insight_data(client, query_id_dict)
        return resp
    else:
        return resp


def create_client(session: boto3.Session, client_name: str):
    client = session.client(client_name)
    return client


def create_session() -> boto3.Session:
    load_dotenv(".env")
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        region_name=os.getenv("AWS_REGION_NAME"),
    )
    return session


def save_file(file_name: str, file_content: str):
    with open(file_name, "w") as f:
        file_content = json.dumps(file_content, indent=1)
        f.write(file_content)
        f.close()


def main():
    client = create_client(create_session(), "logs")
    query_id_dict = send_insight(client)
    print(query_id_dict)
    data = get_insight_data(client, query_id_dict)
    print(data)
    save_file(os.path.join(os.path.dirname(__file__), "query_results.json"), data)


if __name__ == "__main__":
    main()
