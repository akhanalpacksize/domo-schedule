import csv
import logging
import os
import requests
import json
from config.env import SESSION_TOKEN, SCHEDULE_URL
from commons import *
from upload_to_domo import generate_update_schema, upload_csv
from logger_config import setup_logging

# Setup logging
setup_logging(module_name="pull_from_domo")

logger = logging.getLogger(__name__)


def extract_fields(schedule):
    recipients = schedule.get('recipients')
    if recipients is None:
        recipient_emails = [None]
        recipient_user_ids = [None]
    else:
        recipient_emails = [recipient.get('emailAddress', '') for recipient in recipients]
        recipient_user_ids = [str(recipient.get('userId', '')) for recipient in recipients]

    extracted_data = {
        'id': schedule['id'],
        'reportId': schedule['reportId'],
        'reportTitleId': schedule['reportTitleId'],
        'reportTitle': schedule['reportTitle'],
        'reportSubject': schedule['reportSubject'],
        'startTime': schedule['startTime'],
        'endTime': schedule['endTime'],
        'automated': schedule['automated'],
        'cardCount': schedule['cardCount'],
        'attachmentCount': schedule['attachmentCount'],
        'attachmentSize': schedule['attachmentSize'],
        'emailSize': schedule['emailSize'],
        'usingLayout': schedule['usingLayout'],
        'status': schedule['status'],
        'recipient_emails': recipient_emails,
        'recipient_user_ids': recipient_user_ids
    }

    return extracted_data


def get_schedule_info():
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, schedule_report)

    headers = {
        'x-domo-developer-token': SESSION_TOKEN,
        'Accept': 'application/json'
    }
    try:
        response = requests.get(f"{SCHEDULE_URL}/reportschedules/history?filter=ALL&limit=30&skip=0", headers=headers)
        response.raise_for_status()
        response_json = json.loads(response.text)

        extracted_data = []
        for schedule in response_json:
            extracted_data.append(extract_fields(schedule))

        output_file = os.path.join(output_dir, schedule_report)

        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['id', 'reportId', 'reportTitleId', 'reportTitle', 'reportSubject', 'startTime', 'endTime',
                          'automated', 'cardCount', 'attachmentCount', 'attachmentSize', 'emailSize', 'usingLayout',
                          'status', 'recipient_emails', 'recipient_user_ids']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for data in extracted_data:
                for i in range(len(data['recipient_emails'])):
                    writer.writerow({
                        'id': data['id'],
                        'reportId': data['reportId'],
                        'reportTitleId': data['reportTitleId'],
                        'reportTitle': data['reportTitle'],
                        'reportSubject': data['reportSubject'],
                        'startTime': data['startTime'],
                        'endTime': data['endTime'],
                        'automated': data['automated'],
                        'cardCount': data['cardCount'],
                        'attachmentCount': data['attachmentCount'],
                        'attachmentSize': data['attachmentSize'],
                        'emailSize': data['emailSize'],
                        'usingLayout': data['usingLayout'],
                        'status': data['status'],
                        'recipient_emails': data['recipient_emails'][i],
                        'recipient_user_ids': data['recipient_user_ids'][i]
                    })

        logger.info(f"The data has been saved to {schedule_report} successfully.")
    except requests.exceptions.HTTPError as error:
        raise Exception(f"Error: {error.response.status_code}")


get_schedule_info()
generate_update_schema()
upload_csv()
