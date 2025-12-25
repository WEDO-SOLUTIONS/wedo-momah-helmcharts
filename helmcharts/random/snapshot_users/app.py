import os
import re
import json
import logging
import subprocess
import requests
import time
from typing import List, Set
from uuid import UUID
from requests.exceptions import RequestException

# Configure logging
timestamp_format = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt=timestamp_format)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# File handler: INFO and above
file_handler = logging.FileHandler('user_registration.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Console handler: WARNING and above (summary only)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Configuration
CONFIG = {
    "AUTH_TOKEN": os.getenv("X_AUTH_TOKEN", "28ed92f1-de4b-4060-b159-2dc19e9b8bf5"),
    "REGISTER_URL": os.getenv(
        "REGISTER_URL",
        "https://cityview-pro-api-permissions.momah.gov.sa/users/auto_register"
    ),
    "ACTIVATE_URL_TEMPLATE": os.getenv(
        "ACTIVATE_URL_TEMPLATE",
        "https://cityview-pro-api-permissions.momah.gov.sa/users/{user_id}/activate_tariff"
    ),
    "MAX_RETRIES": int(os.getenv("MAX_RETRIES", "3")),
    "RETRY_DELAY": float(os.getenv("RETRY_DELAY", "0.5")),
    "REQUEST_DELAY": float(os.getenv("REQUEST_DELAY", "0.1")),
    "PARTNER_ID": os.getenv("PARTNER_ID", "0"),
    "EMAIL_TEMPLATE": os.getenv(
        "EMAIL_TEMPLATE",
        "{user_id}@momah.gov.sa"
    ),
    "LOG_RETRIES": int(os.getenv("LOG_RETRIES", "3")),
    "LOG_RETRY_DELAY": float(os.getenv("LOG_RETRY_DELAY", "2")),
}

HEADERS = {
    "Accept": "*/*",
    "X-Auth-Token": CONFIG["AUTH_TOKEN"],
    "Content-Type": "application/json"
}

UUID_PATTERN = re.compile(r'user with id ([0-9a-fA-F\-]{36}) must be registered', re.IGNORECASE)

def validate_uuid(uuid_str: str) -> bool:
    try:
        UUID(uuid_str)
        return True
    except ValueError:
        return False


def get_user_ids_from_logs() -> Set[str]:
    user_ids: Set[str] = set()
    try:
        pods = subprocess.run(
            ["kubectl", "--context=production", "-n", "urbi", "get", "pods", "--no-headers", "-o", "name"],
            check=True, stdout=subprocess.PIPE, text=True
        ).stdout.splitlines()
        pods = [p.strip() for p in pods if 'snapshot-pro-ui' in p]
        logging.info("Found %d pods: %s", len(pods), pods)
        for pod in pods:
            logging.info("Fetching logs from %s", pod)
            for attempt in range(1, CONFIG["LOG_RETRIES"] + 1):
                try:
                    res = subprocess.run(
                        ["kubectl", "--context=production", "-n", "urbi", "logs", pod, "--since=24h"],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60
                    )
                    if res.returncode != 0:
                        logging.warning("Log attempt %d for %s failed: %s", attempt, pod, res.stderr.strip() or 'no output')
                    else:
                        for line in res.stdout.splitlines():
                            if 'must be registered' in line.lower():
                                try:
                                    entry = json.loads(line)
                                    msg = entry.get('message', line)
                                except json.JSONDecodeError:
                                    msg = line
                                m = UUID_PATTERN.search(msg)
                                if m and validate_uuid(m.group(1)):
                                    user_ids.add(m.group(1).lower())
                        break
                except subprocess.TimeoutExpired:
                    logging.warning("Timeout fetching logs for %s (attempt %d)", pod, attempt)
                time.sleep(CONFIG["LOG_RETRY_DELAY"])
    except Exception as ex:
        logging.error("Failed extracting user IDs: %s", str(ex), exc_info=True)
        raise
    return user_ids


def call_register_api(user_id: str):
    url = CONFIG["REGISTER_URL"]
    # org_account_id should be the user_id per API spec
    payload = {
        "user_id": user_id,
        "org_account_id": user_id,
        "email": CONFIG["EMAIL_TEMPLATE"].format(user_id=user_id),
        "partner_id": CONFIG["PARTNER_ID"]
    }

    resp = None
    for i in range(1, CONFIG["MAX_RETRIES"] + 1):
        try:
            resp = requests.post(url, headers=HEADERS, json=payload, timeout=10)
            if resp.status_code in (200, 409):
                break
            logging.info("Register attempt %d for %s returned %d: %s", i, user_id, resp.status_code, resp.text)
        except RequestException as e:
            logging.info("Register attempt %d for %s exception: %s", i, user_id, str(e))
        time.sleep(CONFIG["RETRY_DELAY"])
    return resp


def call_activate_api(user_id: str):
    url = CONFIG["ACTIVATE_URL_TEMPLATE"].format(user_id=user_id)
    payload = {
        "tariff_plan_id": "unlimited_v2",
        "territories": "*",
        "rubrics": "",
        "assets": "momrah_pollution,branch,building,demand,population,atm_terminals,traffic_aggregated,urban_environment,population_detailed,station",
        "expiration_date": "2027-04-10T00:00:00",
        "is_tariff_activation": True
    }
    resp = None
    for i in range(1, CONFIG["MAX_RETRIES"] + 1):
        try:
            resp = requests.post(url, headers=HEADERS, json=payload, timeout=10)
            if resp.status_code == 200:
                break
            logging.info("Activate attempt %d for %s returned %d: %s", i, user_id, resp.status_code, resp.text)
        except RequestException as e:
            logging.info("Activate attempt %d for %s exception: %s", i, user_id, str(e))
        time.sleep(CONFIG["RETRY_DELAY"])
    return resp


def process_users(user_ids: List[str]):
    success = 0
    failures = []
    for uid in user_ids:
        logging.info("Processing user %s", uid)
        time.sleep(CONFIG["REQUEST_DELAY"])

        reg = call_register_api(uid)
        if reg and reg.status_code in (200, 409):
            logging.info("Register %s succeeded: %d - %s", uid, reg.status_code, reg.text)
        else:
            code = reg.status_code if reg else 'None'
            text = reg.text if reg else 'No response'
            logging.error("Register %s failed: %s - %s", uid, code, text)

        time.sleep(CONFIG["RETRY_DELAY"])

        act = call_activate_api(uid)
        if act and act.status_code == 200:
            logging.info("Activate %s succeeded: %d - %s", uid, act.status_code, act.text)
            success += 1
        else:
            code = act.status_code if act else 'None'
            text = act.text if act else 'No response'
            logging.error("Activate %s failed: %s - %s", uid, code, text)
            failures.append(uid)

    logging.warning("Processing summary: Success=%d, Failures=%d", success, len(failures))
    if failures:
        logging.warning("Failed users: %s", ", ".join(failures))


def main():
    try:
        logging.info("Starting user registration automation")
        ids = get_user_ids_from_logs()
        if not ids:
            logging.warning("No users to process.")
            return
        logging.info("Found %d users", len(ids))
        process_users(sorted(list(ids)))
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
    except Exception as e:
        logging.error("Fatal error: %s", str(e), exc_info=True)

if __name__ == "__main__":
    main()

