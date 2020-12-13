import json
import unittest
import os
import requests
import sqlite3
import re


# FetchFlights.py
# This peice of code calls two API's to gather data and store it in thee different tables: flights, locals, and COVID under the Data.db database
# The purpose of this peice of code is to fetch the arrivals for the day from the Detroit Airport (400 Arrivals Daily), find where they came from, and then get data about coronavirus positivity from those origins

''' FUNCTIONS '''

# PURPOSE: Figure out if the data.db database has any tables in it and if so how much data is in those tables to know how many flights from the arrivals board to offset 
# INPUT: The cursor to the open database
# OUTPUT: Record Quantity

def databaseValidation(cur):

    try:

        counta = 0
        cur.execute("SELECT * FROM Flights")

        for flight in cur:
            counta += 1

        return counta

    # We need to create tables from scratch if so
    except:
        return 0

# PURPOSE: Get connected to database
# INPUT: The database name
# OUTPUT: A cursor and connection

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

# PURPOSE: Retrive additional context for the origin airport of any given flight
# INPUT: The airport code of any given airport
# OUTPUT: A database ready tuple of the airport code, airport longitude, airport latitude, airport state, and airport country to be used for visulizations and to get COVID-19 data

def getAirportData(code):

    print("Getting Airport Info from FlightAware about " + code)
    data = requests.get("http://traveltimeiosapp:b34cb08f23579a0812280da25b76aee4e47bac16@flightxml.flightaware.com/json/FlightXML3/AirportInfo?airport_code=" + code)
    
    try:

        jsonData = json.loads(data.text)["AirportInfoResult"]
        return (str(jsonData.get("airport_code", code)), float(jsonData.get("longitude", 0.0)), float(jsonData.get("latitude", 0.0)), str(jsonData.get("state", "--")), str(jsonData.get("country_code", "--")))

    except:

        print("Uh-Oh... we are having some trouble connecting to FlightAware")
        return None

# PURPOSE: Get 25 Arrivals from FlightAware Airport Arrivals Boards API
# INPUT: This function takes in how many recent arrivals to offset from the most recent touchdown (this is rapidly changing with every landing)
# OUTPUT: returns the arrivals board for the DTW airport 25 flights at a time

def flightBoardDTW(offset):

    print("Getting 25 Flights from FlightAware Arrivals Board API")
    data = requests.get("http://traveltimeiosapp:b34cb08f23579a0812280da25b76aee4e47bac16@flightxml.flightaware.com/json/FlightXML3/AirportBoards?howMany=25&offset=" + str(offset) + "&airport_code=KDTW&include_ex_data=true&type=arrivals&filter=airline")

    try:
        return json.loads(data.text)["AirportBoardsResult"]["arrivals"]["flights"]

    except:

        print("Uh-Oh... we are having some trouble connecting to FlightAware")
        return None

# PURPOSE: Get coronavirus statistics for a particular state
# INPUT: a particular state
# OUTPUT: basic coronavirus quantities from yesterday or last Friday if weekend

def getCoronaData(state):
    
    print("Getting COVID data for " + state)
    data = requests.get("https://localcoviddata.com/covid19/v1/cases/covidTracking?state=" + state + "&daysInPast=1")

    try:

        dataPack =  json.loads(data.text)["historicData"][0]
        return (state, int(dataPack.get("peoplePositiveNewCasesCt", 0)), int(dataPack.get("peopleNegativeNewCt", 0)), int(dataPack.get("peopleDeathCt", 0)))

    except:

        print("Uh-Oh... we are having some trouble connecting to Muelsoft COVID-19 Data")
        return (state, 0, 0, 0)


# Runner of program that gets flights, loops through them, and calls remaining functions to complete file's mission

def main():

    cur, conn = setUpDatabase("data.db")
    validateDatabaseCount = databaseValidation(cur)
    print("Starting at " + str(validateDatabaseCount) + "ith position in Database\n")

    if(validateDatabaseCount == 0):

       cur.execute("CREATE TABLE IF NOT EXISTS Flights (flightNumber TEXT PRIMARY KEY, origin TEXT, PAXCount INTEGER)")
       cur.execute("CREATE TABLE IF NOT EXISTS Locals (code TEXT PRIMARY KEY, lng DOUBLE, lat DOUBLE, state TEXT, countryCode TEXT)")
       cur.execute("CREATE TABLE IF NOT EXISTS Corona (state TEXT PRIMARY KEY, peoplePositiveNewCasesCt INTEGER, peopleNegativeNewCt INTEGER, peopleDeathCt INTEGER)")

    grabFlights = flightBoardDTW(validateDatabaseCount+2)

    for flight in grabFlights:
      
        # Convience Print to Acknoledge Processing of the Flight
        print("\nProcessing: " + flight["ident"])

        # Inject flight into database accounting for any arrivals that may land while this is being run and push the departures list back
        cur.execute("INSERT OR IGNORE INTO  Flights (flightNumber, origin, PAXCount) VALUES (?, ?, ?) ", (str(flight["ident"]), str(flight["origin"]["code"]), (int(flight.get("seats_cabin_business", "0")) + int(flight.get("seats_cabin_coach", "0")))))
        
        #Get & Store Data about the Airport from the flight
        airportData = getAirportData(str(flight["origin"]["code"]))
        cur.execute("INSERT OR IGNORE INTO Locals (code, lng, lat, state, countryCode) VALUES (?, ?, ?, ?, ?) ", airportData)
        
        #Get Corona Data for all US States that the flight originiated from. International Origins are not supported by the API and are injected into database
        if(airportData[4] == "US"):
            cur.execute("INSERT OR IGNORE INTO Corona (state, peoplePositiveNewCasesCt, peopleNegativeNewCt, peopleDeathCt) VALUES (?, ?, ?, ?) ", getCoronaData(airportData[3]))
       

    conn.commit()
    conn.close()
    

main()