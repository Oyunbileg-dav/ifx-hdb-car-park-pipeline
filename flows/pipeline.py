
from prefect import flow, task
from prefect.utilities.annotations import quote
from etl.extract import fetch_carpark_availability, fetch_carpark_info, fetch_carpark_availability_6pm_historical
from etl.transform import transform_carpark_current_availability, transform_carpark_info, transform_carpark_availability_6pm_historical
from etl.load import load_carpark_current_availability, load_carpark_info, load_carpark_availability_6pm_last_30days
from etl.reports import generate_analysis_report

@task
def extract_carpark_current_availability():
    return fetch_carpark_availability()

@task
def extract_carpark_info():
    return fetch_carpark_info()

@task
def extract_carpark_availability_6pm_historical():
    return fetch_carpark_availability_6pm_historical()

@task
def transform_carpark_current_availability_task(raw_data):
    return transform_carpark_current_availability(raw_data)

@task
def transform_carpark_info_task(raw_data):
    return transform_carpark_info(raw_data)

@task
def transform_carpark_availability_6pm_historical_task(raw_data):
    return transform_carpark_availability_6pm_historical(raw_data)

@task
def load_carpark_current_availability_task(records):
    return load_carpark_current_availability(records)

@task
def load_carpark_info_task(records):
    return load_carpark_info(records)

@task
def load_carpark_availability_6pm_last_30days_task(records):
    return load_carpark_availability_6pm_last_30days(records)

@flow
def daily_pipeline():
    raw_data = extract_carpark_current_availability()
    records = transform_carpark_current_availability_task(quote(raw_data))
    load_carpark_current_availability_task(quote(records))
    raw_data = extract_carpark_info()
    records = transform_carpark_info_task(quote(raw_data))
    load_carpark_info_task(quote(records))
    raw_data = extract_carpark_availability_6pm_historical()
    records = transform_carpark_availability_6pm_historical_task(quote(raw_data))
    load_carpark_availability_6pm_last_30days_task(quote(records))
    generate_analysis_report()

if __name__ == "__main__":
    daily_pipeline()
