import os
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Configuration
SENDGRID_API_KEY = Variable.get('SENDGRID_API_KEY')
SENDER_EMAIL = Variable.get('SENDER_EMAIL')
RECIPIENT_EMAIL = Variable.get('RECIPIENT_EMAIL')
UNESCO_API_TOKEN = Variable.get('UNESCO_API_TOKEN')

API_URL = "https://data.dev-wins.com/api/action/group_show?id=member-states&include_groups=True"
UNESCO_API_URL = "https://www.unesco.org/strapi/api/countries"
UNESCO_AUTH_TOKEN = f"Bearer {UNESCO_API_TOKEN}"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [RECIPIENT_EMAIL],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def normalize_name(name):
    return name.lower().replace('å','-').replace('\u00c5','-').replace('\u00fc','-')\
        .replace('\u00f4','-').replace('\u00e7','-').replace(' ', '-')\
        .replace("'",'-').replace('(', '').replace(')', '').replace(',', '').strip('-')

def compare_countries(**kwargs):
    # Retrieve data from the API at data.dev-wins.com
    api_response = requests.get(API_URL)
    if api_response.status_code != 200:
        print(f"Failed to retrieve API data: {api_response.status_code}")
        return
    api_data = api_response.json()

    # Extract group names
    group_names = [group['name'] for group in api_data['result']['groups']]

    # Retrieve UNESCO Member States data from UNESCO API
    headers = {
        "Authorization": UNESCO_AUTH_TOKEN
    }
    unesco_response = requests.get(UNESCO_API_URL, headers=headers)
    if unesco_response.status_code != 200:
        print(f"Failed to retrieve UNESCO data: {unesco_response.status_code}")
        return
    unesco_data = unesco_response.json()

    # Create a dictionary of normalized UNESCO countries
    unesco_countries = {}
    for country in unesco_data['data']:
        title_en = country['attributes']['title_en']
        unesco_countries[normalize_name(title_en)] = title_en

    # Compare groups with UNESCO countries
    matches = []
    mismatches = []

    for group in group_names:
        normalized_group = normalize_name(group)
        if normalized_group in unesco_countries:
            matches.append((group, unesco_countries[normalized_group]))
        else:
            mismatches.append(group)

    # Print results of the first comparison
    print("1. Comparison of API groups with UNESCO countries:")
    print(f"Matches found: {len(matches)}")
    print(f"Groups without matches: {len(mismatches)}")

    print("\nAPI groups without matching UNESCO countries:")
    for i, group in enumerate(mismatches, 1):
        print(f"{i}. {group}")

    # Create sets of normalized countries
    api_countries = set(normalize_name(group) for group in group_names)
    unesco_country_set = set(unesco_countries.keys())

    # Find UNESCO countries not in the API
    countries_not_in_api = unesco_country_set - api_countries

    # Print results of the second comparison
    print("\n2. Countries in UNESCO but not in the API:")
    print(f"Count: {len(countries_not_in_api)}")

    print("\nList of countries in UNESCO but not in the API:")
    for i, country in enumerate(sorted(countries_not_in_api), 1):
        original_name = unesco_countries[country]
        print(f"{i}. {original_name} (normalized: {country})")

    # Send an email if there are UNESCO countries not in the API (Case 2)
    if countries_not_in_api:
        missing_countries_list = '\n'.join([f"{unesco_countries[country]}" for country in sorted(countries_not_in_api)])

        message_case2 = Mail(
            from_email=SENDER_EMAIL,
            to_emails=RECIPIENT_EMAIL,
            subject='UNESCO Countries Missing in API',
            plain_text_content=f'The following UNESCO countries are not present in the API:\n\n' + missing_countries_list
        )

        try:
            sg = SendGridAPIClient(SENDGRID_API_KEY)
            response = sg.send(message_case2)
            print("\nEmail sent successfully for UNESCO countries missing in API.")
        except Exception as e:
            print(f"\nAn error occurred while sending the email for UNESCO missing countries: {e}")

with DAG(
    'countries_comparison_dag',
    default_args=default_args,
    description='DAG to compare API countries with UNESCO and send emails',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 10, 23),
    catchup=False,
) as dag:

    compare_task = PythonOperator(
        task_id='compare_countries',
        python_callable=compare_countries,
        provide_context=True,
    )

    compare_task
