import sqlite3 as lite
import os

from geopy import geocoders

import distance

DIR = os.path.dirname(__file__)
MAX_SHORT_HAUL_KM = 3700

''' Travel functions '''


def air_ghg(origin=None, destination=None, ghg_units="kg CO2e", haul=None,
            passenger_class="Class", radiative_forcing=True):
    flights = Flights()
    if not haul:
        if not origin and not destination:
            raise Exception("Cannot determine haul of flight. Either specify haul, or origin and destination.")
        else:
            haul = get_haul(origin, destination)
    criteria = {"GHGUnits": ghg_units, "Haul": haul,
                "Class": passenger_class, "RF": radiative_forcing}
    return flights.get_factor(criteria)


def bus_ghg(ghg_units="kg CO2e", bus_type="AverageLocalBus"):
    bus = Bus()
    criteria = {"GHGUnits": ghg_units, "BusType": bus_type}
    return bus.get_factor(criteria)


def car_ghg(ghg_units="kg CO2e", select_by="Size",
            size="Average", market_segment="UpperMedium",
            fuel='Unknown', unit='km'):
    if select_by == "Size":
        cars = Car_Size()
        criteria = {"GHGUnits": ghg_units, "Size": size, "Unit": unit, "Fuel": fuel}
        return cars.get_factor(criteria)
    elif select_by == "MarketSegment":
        cars = Car_Type()
        criteria = {"GHGUnits": ghg_units, "MarketSegment": market_segment, "Unit": unit, "Fuel": fuel}
        return cars.get_factor(criteria)
    else:
        raise Exception("%s is not a valid criterion for car_ghg selection" % select_by)


def motorbike_ghg(ghg_units="kg CO2e", size="Average", unit='km'):
    motorbike = Motorbike()
    criteria = {"GHGUnits": ghg_units, "Size": size, "Unit": unit}
    return motorbike.get_factor(criteria)


def rail_ghg(ghg_units="kg CO2e", rail_type="NationalRail"):
    trains = Train()
    criteria = {"GHGUnits": ghg_units, "RailType": rail_type}
    return trains.get_factor(criteria)


def taxi_ghg(ghg_units="kg CO2e", taxi_type="RegularTaxi", units="PassengerKm"):
    taxi = Taxi()
    return taxi.get_factor({"GHGUnits": ghg_units, "TaxiType": taxi_type, "PassengerKm": units})


def get_country(location):
    g = geocoders.GoogleV3()
    place, _geoid = g.geocode(location)
    country = place.split(',')[-1]
    return country


def get_haul(origin, destination):
    if get_country(origin) == "UK" and get_country(destination) == "UK":
        return "Domestic"
    elif distance.air_distance(origin, destination, 'km') < MAX_SHORT_HAUL_KM:
        return "ShortHaul"
    else:
        return "LongHaul"


class ActivityTable:

    @property
    def columns(self):
        with lite.connect(os.path.join(DIR, 'Databases/carbon_emissions.db')) as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM %s" % self.table_name)
            names = list(map(lambda x: x[0], cur.description))
            return names

    @property
    def ghg_units(self):
        with lite.connect(os.path.join(DIR, 'Databases/carbon_emissions.db')) as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM %s" % self.table_name)
            names = list(map(lambda x: x[0], cur.description))
            return [nm for nm in names if 'kg' in nm]

    @property
    def options(self):
        with lite.connect(os.path.join(DIR, 'Databases/carbon_emissions.db')) as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM %s" % self.table_name)
            names = list(map(lambda x: x[0], cur.description))
            return [nm for nm in names if 'kg' not in nm]

    def get_factor(self, criteria):
        query, error_message = self.select_from_criteria(criteria)
        with lite.connect(os.path.join(DIR, 'Databases/carbon_emissions.db')) as con:
            cur = con.cursor()
            cur.execute(query)
            row = cur.fetchone()
            if row:
                return row[0]
            else:
                raise Exception(error_message)

    def select_from_criteria(self, criteria):
        ghg_units = criteria.pop('GHGUnits')
        # extract any booleans
        true_bools = [i for i in criteria if criteria[i] is True]
        false_bools = [i for i in criteria if criteria[i] is False]
        # extract any booleans and numbers (booleans first as booleans can be mistaken for ints)
        for b in true_bools:
            criteria.pop(b)
        for b in false_bools:
            criteria.pop(b)
        numbers = {i: criteria[i] for i in criteria
                   if isinstance(criteria[i], int) or isinstance(criteria[i], float)}
        for n in numbers:
            criteria.pop(n)

        str_params = ["%s=\"%s\"" % (k, criteria[k]) for k in criteria]
        num_params = ["%s=\"%s\"" % (k, numbers[k]) for k in numbers]
        query = "SELECT %s FROM %s" % (ghg_units, self.table_name)
        if str_params or num_params or true_bools:
            query += " WHERE "
            if str_params:
                query += " AND ".join(str_params)
            if num_params:
                query += " AND " + " AND ".join(num_params)
            if true_bools:
                query += " AND " + " AND ".join(true_bools)
        error_message = 'Error selecting from database'
        return query, error_message


class Bus(ActivityTable):

    def __init__(self):
        self.table_name = "Bus"


class Car_Type(ActivityTable):

    def __init__(self):
        self.table_name = "Car_Type"


class Car_Size(ActivityTable):

    def __init__(self):
        self.table_name = "Car_Size"


class Flights(ActivityTable):

    def __init__(self):
        self.table_name = "Flights"


class Motorbike(ActivityTable):

    def __init__(self):
        self.table_name = "Motorbike"


class Train(ActivityTable):

    def __init__(self):
        self.table_name = "Train"


class Taxi(ActivityTable):

    def __init__(self):
        self.table_name = "Taxi"


def main():
    if __name__ == "__main__":
        main()

