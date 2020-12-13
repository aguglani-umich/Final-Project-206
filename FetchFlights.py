import json
import unittest
import os
import requests
import sqlite3
import re


# FetchFlights.py
# This peice of code calls two API's to gather data and store it in thee different tables: flights, locals, and COVID under the Data.db database


''' CONVIENCE FUNCTIONS '''

def databaseValidation(cur):

    try:

        counta = 0
        cur.execute("SELECT * FROM Flights")

        for flight in cur:
            counta += 1

        return counta

    except:
        return 0

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def getAirportData(code):

    print("Getting Airport Info from FlightAware about " + code)
    data = requests.get("http://traveltimeiosapp:b34cb08f23579a0812280da25b76aee4e47bac16@flightxml.flightaware.com/json/FlightXML3/AirportInfo?airport_code=" + code)
    
    try:

        jsonData = json.loads(data.text)["AirportInfoResult"]
        return (str(jsonData.get("airport_code", code)), float(jsonData.get("longitude", 0.0)), float(jsonData.get("latitude", 0.0)), str(jsonData.get("state", "--")), str(jsonData.get("country_code", "--")))

    except:

        print("Uh-Oh... we are having some trouble connecting to FlightAware")
        return None

# Get 25 Arrivals from FlightAware Airport Boards API
# This function takes in how many recent arrivals to offset from and returns the arrivals board for the DTW airport

def flightBoardDTW(offset):

    print("Getting Flights from FlightAware")
    data = requests.get("http://traveltimeiosapp:b34cb08f23579a0812280da25b76aee4e47bac16@flightxml.flightaware.com/json/FlightXML3/AirportBoards?howMany=25&offset=" + str(offset) + "&airport_code=KDTW&include_ex_data=true&type=arrivals&filter=airline")

    try:
        return json.loads(data.text)["AirportBoardsResult"]["arrivals"]["flights"]

    except:

        print("Uh-Oh... we are having some trouble connecting to FlightAware")
        return None

def main():

    cur, conn = setUpDatabase("data.db")
    validateDatabaseCount = databaseValidation(cur)
    print(validateDatabaseCount)

    if(validateDatabaseCount == 0):

       cur.execute("DROP TABLE IF EXISTS Flights")
       cur.execute("DROP TABLE IF EXISTS Locals")
       cur.execute("DROP TABLE IF EXISTS Corona")
       cur.execute("CREATE TABLE IF NOT EXISTS Flights (flightNumber TEXT PRIMARY KEY, origin TEXT, PAXCount INTEGER)")
       cur.execute("CREATE TABLE IF NOT EXISTS Locals (code TEXT PRIMARY KEY, lng DOUBLE, lat DOUBLE, state TEXT, countryCode TEXT)")
       cur.execute("CREATE TABLE IF NOT EXISTS Corona (state TEXT PRIMARY KEY, peoplePositiveNewCasesCt INTEGER, peopleNegativeNewCt INTEGER, peopleDeathCt INTEGER)")

    grabFlights = flightBoardDTW(validateDatabaseCount+2)

    for flight in grabFlights:
        #print(flight)
        print((flight["ident"], flight["origin"]["code"], (int(flight.get("seats_cabin_business", "0")) + int(flight.get("seats_cabin_coach", "0")))))
        cur.execute("INSERT OR IGNORE INTO  Flights (flightNumber, origin, PAXCount) VALUES (?, ?, ?) ", (str(flight["ident"]), str(flight["origin"]["code"]), (int(flight.get("seats_cabin_business", "0")) + int(flight.get("seats_cabin_coach", "0")))))
        cur.execute("INSERT OR IGNORE INTO Locals (code, lng, lat, state, countryCode) VALUES (?, ?, ?, ?, ?) ", getAirportData(str(flight["origin"]["code"])))

        # insert into flights table
        # insert airport data into Locals Table using data from an indvidual flight packet
        # use State Code to launch and get corona data and insert result into corona table if and only if the state is not already supported

    conn.commit()
    conn.close()
    

main()