import os
import requests
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from base64 import b64encode
from config.env import *


# GETS ACCESS TOKEN FOR DOMO
def get_access_token():
    auth_headers = {
        'Authorization': 'Basic ' + b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()
    }

    payload = {
        "grant_type": GRANT_TYPE,
    }

    try:
        response = requests.post(AUTH_URL, data=payload, headers=auth_headers)
        response.raise_for_status()
        response_json = response.json()
        access_token = response_json['access_token']
        return access_token

    except requests.exceptions.HTTPError as e:
        error_message = e.response.json().get("error_description", "Unknown error")
        raise Exception(f"Authentication error: {e.response.status_code} - {error_message}")


# get Local instance access token
def get_local_access_token():
    auth_headers = {
        'Authorization': 'Basic ' + b64encode(f'{LOCAL_CLIENT_ID}:{LOCAL_CLIENT_SECRET}'.encode()).decode()
    }

    payload = {
        "grant_type": GRANT_TYPE
    }

    try:
        response = requests.post(AUTH_URL, data=payload, headers=auth_headers)
        response.raise_for_status()
        response_json = response.json()
        access_token = response_json['access_token']
        return access_token

    except requests.exceptions.HTTPError as e:
        error_message = e.response.json().get("error_description", "Unknown error")
        raise Exception(f"Authentication error: {e.response.status_code} - {error_message}")


def send_email(subject, body_data, sender, recipients, password):
    # Retrieve the file path of the currently executing script
    script_path = os.path.abspath(__file__)

    # Extract the project directory name from the file path
    project_name = os.path.basename(os.path.dirname(script_path))

    body_template = """
    Greetings Team,

   An error occurred in the {} that requires immediate attention.
   Here are the key details:
    
    Error Message: {}
    
    Timestamp: {}
    
    Our team is actively resolving this issue promptly and is fully committed to minimizing its impact on our operations.
    
    We apologize for any inconvenience caused and appreciate your cooperation during this time.

    Best regards,
    IS Team
    """
    body = body_template.format(project_name, *body_data)

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ','.join(recipients)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, recipients, msg.as_string())

    print("Email sent!")


def send_email_error(error_message):
    # Send error email notification
    subject = "Error Report"
    body_data = [error_message, datetime.utcnow().isoformat() + 'Z']
    sender = EMAIL
    recipients = RECIPIENTS
    password = GOOGLE_PASSWORD

    send_email(subject, body_data, sender, recipients, password)
