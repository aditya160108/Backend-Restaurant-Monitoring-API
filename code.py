# Step 1: Importing modules
import os
import csv
import random
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from pytz import timezone
import sqlite3

app = Flask(__name__)
DB_NAME = "restaurant_monitor.db"
REPORTS_DIR = "reports"

# Step 2: Create a database to store the CSV data
def create_database():
    with sqlite3.connect(DB_NAME) as conn:
        c = conn.cursor()

        # Create table for the store status data
        c.execute("CREATE TABLE IF NOT EXISTS store_status (store_id INTEGER, timestamp_utc TEXT, status TEXT)")

        # Create table for the store business hours data
        c.execute("CREATE TABLE IF NOT EXISTS store_hours (store_id INTEGER, dayOfWeek INTEGER, start_time_local TEXT, end_time_local TEXT)")

        # Create table for the store timezones data
        c.execute("CREATE TABLE IF NOT EXISTS store_timezones (store_id INTEGER, timezone_str TEXT)")

        conn.commit()

# Step 3: Load the data from the CSV files into the database
def load_data():
    with sqlite3.connect(DB_NAME) as conn:
        c = conn.cursor()

        # Load store status data
        with open("store_status.csv", "r") as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            c.executemany("INSERT INTO store_status VALUES (?, ?, ?)", (map(str.strip, row) for row in reader))

        # Load store business hours data
        with open("store_hours.csv", "r") as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            c.executemany("INSERT INTO store_hours VALUES (?, ?, ?, ?)", (map(str.strip, row) for row in reader))

        # Load store timezones data
        with open("store_timezones.csv", "r") as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            c.executemany("INSERT INTO store_timezones VALUES (?, ?)", (map(str.strip, row) for row in reader))

        conn.commit()

# Helper function to convert local time to UTC time
def local_to_utc(local_time, timezone_str):
    local_tz = timezone(timezone_str)
    local_dt = local_tz.localize(local_time)
    utc_dt = local_dt.astimezone(timezone('UTC'))
    return utc_dt

# Step 5: Generate the report by querying the database and calculating the required metrics
def generate_report():
    with sqlite3.connect(DB_NAME) as conn:
        c = conn.cursor()

        # Get the current timestamp as the max timestamp among all observations
        c.execute("SELECT MAX(timestamp_utc) FROM store_status")
        max_timestamp = c.fetchone()[0]

        # Get distinct store IDs
        c.execute("SELECT DISTINCT store_id FROM store_status")
        store_ids = c.fetchall()

        # Initialize the report data
        report_data = []

        # Iterate over each store
        for store_id in store_ids:
            store_id = store_id[0]

            # Get the business hours for the store
            c.execute("SELECT day, start_time_local, end_time_local FROM store_hours WHERE store_id=?", (store_id,))
            business_hours = c.fetchall()

            # Initialize the metrics
            uptime_last_hour = 0
            uptime_last_day = 0
            update_last_week = 0
            downtime_last_hour = 0
            downtime_last_day = 0
            downtime_last_week = 0

            # Iterate over the timestamps for the relevant time intervals
            for interval_start, interval_end in [(max_timestamp - timedelta(hours=1), max_timestamp),
                                                 (max_timestamp - timedelta(days=1), max_timestamp),
                                                 (max_timestamp - timedelta(weeks=1), max_timestamp)]:
                # Get the store status observations within the interval
                c.execute("SELECT timestamp_utc, status FROM store_status WHERE store_id=? AND timestamp_utc>=? AND timestamp_utc<=?",
                          (store_id, interval_start, interval_end))
                observations = c.fetchall()

                # Calculate uptime and downtime within business hours
                for observation in observations:
                    timestamp_utc = datetime.strptime(observation[0], "%Y-%m-%d %H:%M:%S")
                    status = observation[1]

                    # Convert UTC timestamp to local time using store's timezone
                    c.execute("SELECT timezone_str FROM store_timezones WHERE store_id=?", (store_id,))
                    timezone_str = c.fetchone()[0]
                    local_time = timestamp_utc.astimezone(timezone(timezone_str))

                    # Iterate over the business hours to check for overlap
                    for dayOfWeek, start_time_local, end_time_local in business_hours:
                        dayOfWeek = int(dayOfWeek)
                        start_time_local = datetime.strptime(start_time_local, "%H:%M:%S").time()
                        end_time_local = datetime.strptime(end_time_local, "%H:%M:%S").time()

                        # Create datetime objects for start and end times
                        start_time = datetime.combine(local_time.date(), start_time_local)
                        end_time = datetime.combine(local_time.date(), end_time_local)

                        # Check if the observation falls within the business hours
                        if local_time.weekday() == dayOfWeek and start_time <= local_time <= end_time:
                            if status == "active":
                                if interval_start == max_timestamp - timedelta(hours=1):
                                    uptime_last_hour += 1
                                uptime_last_day += 1
                                update_last_week += 1
                            else:
                                if interval_start == max_timestamp - timedelta(hours=1):
                                    downtime_last_hour += 1
                                downtime_last_day += 1
                                downtime_last_week += 1

            # Append the metrics to the report data
            report_data.append((store_id, uptime_last_hour, uptime_last_day, update_last_week,
                                downtime_last_hour, downtime_last_day, downtime_last_week))

    return report_data

# Step 6: Save the report data to a CSV file
def save_report_to_csv(report_data):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_id = generate_report_id()
    report_filename = f"{REPORTS_DIR}/{report_id}.csv"

    with open(report_filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["store_id", "uptime_last_hour", "uptime_last_day", "update_last_week",
                         "downtime_last_hour", "downtime_last_day", "downtime_last_week"])
        writer.writerows(report_data)

    return report_id

# Helper function to generate a random report ID
def generate_report_id():
    return f"report_{random.randint(1000, 9999)}"

# Step 7: Implement the trigger_report API endpoint
@app.route("/trigger_report", methods=['POST'])
def trigger_report():
    create_database()
    load_data()
    report_data = generate_report()
    report_id = save_report_to_csv(report_data)

    # Return the report_id as a JSON response
    return jsonify({"report_id": report_id})
    
@app.route("/", methods=['GET'])
def home():
	return "Hello World"

if __name__ == "__main__":
    app.debug = True
    app.run(host='0.0.0.0', port=5000)
