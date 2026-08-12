import calendar
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import timetable

class Stop:
    def __init__(self, stop):


        self.stopCode = stop[0]
        self.districtCode = stop[1]
        self.commonName = stop[2]
        self.landmark = stop[3]
        self.street = stop[4]
        self.indicator = stop[5]
        self.directionality = stop[6]
        self.locality = stop[7]
        self.parentLocality = stop[8]
        self.grandparentLocality = stop[9]
        self.town = stop[10]
        self.longitude = stop[11]
        self.latitude = stop[12]

def execute():
    conn = sqlite3.connect('.out/stops.db')
    c = conn.cursor()

    c.execute('''drop table if exists stops''')
    c.execute('''create table stops (stopCode text primary key, districtCode text, commonName text, landmark text, street text, indicator text, directionality text, locality text, parentLocality text, grandparentLocality text, town text, longitude real, latitude real)''')
    file = pd.read_csv('.data/stops.csv').values.tolist()

    for row in file:
        stopCode = row[0]
        townCode = row[17]
        commonName = row[4]
        landmark = row[8]
        street = row[10]
        indicator = row[14]
        bearing = row[16]
        locality = row[18]
        parentLocality = row[19]
        grandparentLocality = row[20]
        town = row[21]
        longitude = row[29]
        latitude = row[30]

        c.execute('''insert into stops values (?,?,?,?,?,?,?,?,?,?,?,?,?)''', (stopCode, townCode, commonName, landmark, street, indicator, bearing, locality, parentLocality, grandparentLocality, town, longitude, latitude))

    conn.commit()
    conn.close()

def fullNameStop(stop):

    stop = Stop(stop)
    locationBasedIndicators = ["at", "opp", "adj", "o/s", "nr"]
    locBasedIndicatorsFull = {"at":"at", "opp":"opposite", "adj":"adjacent", "o/s":"outside", "nr":"near"}
    directionalityFull = {"N": "north", "S": "south", "E": "east", "W": "west", "NE":"north-east", "SE":"south-east", "SW":"south-west", "NW":"north-west"}
    output = " "
    if stop.indicator in locationBasedIndicators:
        output += stop.locality +", " + locBasedIndicatorsFull[stop.indicator] + " " + stop.commonName + "\n"
        if stop.street:
            if stop.landmark:
                output += " on " + stop.street + ", near " + stop.landmark + "\n"
            else:
                output += " on " + stop.street
    else:
        if stop.indicator:
            output += stop.commonName + " ("+stop.indicator+")" + "\n"
        else:
            output += stop.commonName + "\n"
        if stop.street:
            if stop.landmark:
                output += " on "+ stop.street+", near "+ stop.landmark + "\n"
            else:
                output += " on " + stop.street
    if stop.directionality:
        output += " Buses head "+directionalityFull[stop.directionality]+"\n"
    return output

def _arrival_datetime_on_date(arrival_hms, day):
    hour, minute, second = map(int, arrival_hms.split(":"))
    if hour >= 24:
        return None
    return datetime.combine(day, datetime.min.time()).replace(
        hour=hour, minute=minute, second=second,
    )

def _next_arrival_for_trip(service_ref, vehicle_journey_code, stop_code, from_time, max_days=14):
    """Earliest future arrival for one trip, searching up to max_days ahead."""
    for day_offset in range(max_days):
        check_day = from_time.date() + timedelta(days=day_offset)
        if not timetable.determineIfRouteRunsOnDate(
            service_ref, vehicle_journey_code, check_day,
        ):
            continue

        arrival_hms = timetable.whenDoesThisTripGetToThisStopCode(
            service_ref, vehicle_journey_code, stop_code,
        )
        candidate = _arrival_datetime_on_date(arrival_hms, check_day)
        if candidate is not None and candidate >= from_time:
            return candidate

    return None


def checkNextBus(stopCode, from_time=None, max_days=14):
    """Next arrival time per service that calls at this stop."""
    if from_time is None:
        from_time = datetime.now()

    conn = sqlite3.connect(".out/stops.db", timeout=60)
    timetable.configure_sqlite_connection(conn, write=False)
    try:
        c = conn.cursor()
        c.execute(
            "select distinct Operator, RouteName from busstops where StopID=?",
            (stopCode,),
        )
        routes = c.fetchall()
    finally:
        conn.close()

    arriv = {}
    for operator, route_name in routes:
        service_ref = f"{operator} {route_name}"
        route_conn = timetable.open_sqlite_database(
            timetable.service_ref_path(service_ref), readonly=True,
        )
        try:
            route_cursor = route_conn.cursor()
            route_cursor.execute(
                "select distinct vehicleJourneyCode from busRuns where stopPointRef=?",
                (stopCode,),
            )
            runs = route_cursor.fetchall()
        finally:
            route_conn.close()

        for (vehicle_journey_code,) in runs:
            candidate = _next_arrival_for_trip(
                service_ref, vehicle_journey_code, stopCode, from_time, max_days,
            )
            if candidate is None:
                continue
            if service_ref not in arriv or candidate < arriv[service_ref]:
                arriv[service_ref] = candidate

    return arriv


def _format_next_arrival(service_ref, arrival, now):
    if arrival.date() == now.date():
        when = "today"
    elif arrival.date() == now.date() + timedelta(days=1):
        when = "tomorrow"
    else:
        when = arrival.strftime("%A %d %B")

    return f"The next {service_ref} is at {arrival.strftime('%H:%M:%S')} {when}"


def listBusesWhichCome(stopCode):
    now = datetime.now()
    arriv = checkNextBus(stopCode, now)

    if not arriv:
        print("No buses found for this stop in the next 14 days.")
        return

    for service_ref in sorted(arriv):
        print(_format_next_arrival(service_ref, arriv[service_ref], now))

def findStopCode():
    conn = sqlite3.connect('.out/stops.db')
    c = conn.cursor()

    name = input("Enter stop name: ")
    locality = input("Enter town or city name: ")

    c.execute('''select * from stops where commonName like ? COLLATE NOCASE and locality like ? COLLATE NOCASE''', (f"%{name}%", f"%{locality}%"))
    possibilities = c.fetchall()

    if len(possibilities) != 0:

        for possibility in range(len(possibilities)):
            print(str(possibility)+":")
            fullNameStop(possibilities[possibility])

if __name__ == '__main__':
    listBusesWhichCome("627007020350")