import json
import time
import logging
import csv
import io
import os
import random
import boto3
import requests
import jwt
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from requests.exceptions import RequestException

# ---------------- CONFIG ---------------- #

SECRET_NAME = os.environ["SECRET_NAME"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "salesforce/limits/")
SALESFORCE_LOGIN_URL = "https://test.salesforce.com"
API_VERSION = "v60.0"

HTTP_TIMEOUT = 10
MAX_RETRIES = 3
BACKOFF_BASE = 2

REQUIRED_KPIS = [
    "DailyApiRequests",
    "DailyBulkApiRequests",
    "DailyStreamingApiEvents",
    "ConcurrentAsyncGetReportInstances",
    "ConcurrentSyncReportRuns",
    "DailyDurableGenericStreamingApiEvents",
    "HourlySyncReportRuns"
]

# --------------- LOGGING ---------------- #

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------- AWS CLIENTS ------------ #

secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

# --------------- UTILITIES -------------- #

def log_event(event_type, message, extra=None):
    log_payload = {
        "event": event_type,
        "message": message,
        "timestamp": datetime.utcnow().isoformat()
    }
    if extra:
        log_payload.update(extra)
    logger.info(json.dumps(log_payload))


def emit_metric(metric_name, value):
    cloudwatch.put_metric_data(
        Namespace="SalesforceLimitsPipeline",
        MetricData=[
            {
                "MetricName": metric_name,
                "Value": value,
                "Unit": "Count"
            }
        ]
    )


def retry_with_backoff(func, *args):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return func(*args)
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            sleep_time = (BACKOFF_BASE ** attempt) + random.uniform(0, 1)
            log_event("retry", f"Retrying attempt {attempt}", {"error": str(e)})
            time.sleep(sleep_time)


# --------------- CORE FUNCTIONS ---------- #

def get_secret():
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response["SecretString"])


def generate_jwt_token(consumer_key, username, private_key):
    now = int(time.time())
    payload = {
        "iss": consumer_key,
        "sub": username,
        "aud": SALESFORCE_LOGIN_URL,
        "exp": now + 300
    }
    return jwt.encode(payload, private_key, algorithm="RS256")


def get_access_token(jwt_token):
    url = f"{SALESFORCE_LOGIN_URL}/services/oauth2/token"

    response = requests.post(
        url,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt_token
        },
        timeout=HTTP_TIMEOUT
    )

    if response.status_code == 401:
        raise Exception("JWT authentication failed")

    response.raise_for_status()

    data = response.json()
    if "access_token" not in data:
        raise Exception("Invalid OAuth response")

    return data["access_token"], data["instance_url"]


def get_limits(access_token, instance_url):
    url = f"{instance_url}/services/data/{API_VERSION}/limits"

    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=HTTP_TIMEOUT
    )

    if response.status_code in [429, 500, 502, 503, 504]:
        raise Exception(f"Salesforce transient error: {response.status_code}")

    response.raise_for_status()
    return response.json()


def filter_kpis(limits_json):
    filtered = {}

    for kpi in REQUIRED_KPIS:
        if kpi in limits_json:
            filtered[f"{kpi}_Max"] = limits_json[kpi].get("Max")
            filtered[f"{kpi}_Remaining"] = limits_json[kpi].get("Remaining")

    if len(filtered) != len(REQUIRED_KPIS) * 2:
        raise Exception("Missing expected KPI fields")

    return filtered


def upload_to_s3(data_dict):
    utc_now = datetime.now(timezone.utc)
    partition = utc_now.strftime("%Y/%m/%d/%H/%M")
    key = f"{S3_PREFIX}{partition}/sf_limits.csv"

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=data_dict.keys())
    writer.writeheader()
    writer.writerow(data_dict)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )

    log_event("s3_upload", "Upload successful", {"key": key})


# --------------- HANDLER ---------------- #

def lambda_handler(event, context):
    start_time = time.time()

    try:
        log_event("start", "Lambda execution started")

        secret = get_secret()

        jwt_token = generate_jwt_token(
            secret["consumer_key"],
            secret["username"],
            secret["private_key"]
        )

        access_token, instance_url = retry_with_backoff(
            get_access_token, jwt_token
        )

        limits_json = retry_with_backoff(
            get_limits, access_token, instance_url
        )

        filtered_data = filter_kpis(limits_json)

        upload_to_s3(filtered_data)

        duration = round(time.time() - start_time, 2)

        emit_metric("SuccessfulRuns", 1)
        log_event("success", "Execution completed", {"duration_seconds": duration})

        return {"statusCode": 200}

    except Exception as e:
        emit_metric("FailedRuns", 1)
        log_event("failure", "Execution failed", {"error": str(e)})
        raise