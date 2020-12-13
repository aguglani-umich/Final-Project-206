import json
import unittest
import os
import requests
import sqlite3
import re


# GenerateResults.py
# This peice of code combines the database tables and generates a list of COVID origin airport vectors from around the country into DTW

''' Data '''
vectors = []

''' FUNCTIONS '''

# PURPOSE: Clean data from FetchFlights.py into a usable format. Removes all intl departures and feilds with empty values
# INPUT: Cursor for database
# OUTPUT: The vectors list is populated with a list of origin airports (vectors) and the accociated COVID data for those origin vectors

def cleanAndJoin(cur):
    cur.execute("Select Locals.code, Locals.lng, Locals.lat, Locals.state, Locals.cityName, Locals.countryCode, Corona.peoplePositiveNewCasesCt, Corona.peopleNegativeNewCt, Corona.peopleDeathCt from Locals join Corona on Locals.state = Corona.state")
    for peice in cur:
        vectors.append(list(peice))
        


# PURPOSE: Figure out the danger level of a particular airport origin vector
# INPUT: Cursor for the database
# OUTPUT: The vectors list is modified with additional datafields such as frequency of flights for that vector, positivity rate, vectorScore, and Warning Label

def calculateVectorItensity(cur):

    for flight in vectors:
        cur.execute("SELECT * FROM Flights WHERE origin=?", (flight[0],))
        freq = len(cur.fetchall())
        posRate = float(flight[6])/flight[7]
        vectorScore = freq * (float(flight[6])/flight[7]) * 100
        flight.append(freq)
        flight.append(posRate)
        flight.append(vectorScore)
        
        if(vectorScore < 20):
            flight.append("LOW")
        elif (vectorScore < 100):
            flight.append("MED")
        elif (vectorScore < 200):
            flight.append("HIGH")
        else:
            flight.append("VERY HIGH")

# PURPOSE: Get connected to database
# INPUT: The database name
# OUTPUT: A cursor and connection

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

# PURPOSE: Use vectors list to print out a summary of calculated statistics
# INPUT: The intended output file name
# OUTPUT: A list of all the vectors and a summary of calculated statistics

def outputVectorsToFile(filename):

    # open the output file for writing
    dir = os.path.dirname(__file__)
    outFile = open(os.path.join(dir, filename), "w")
    outFile.write("Detroit Airport Covid Vectors for 24 Hours\n")
    outFile.write("\nA vector is an origin point for COVID-19 coming to the Detroit Airport.\nWe look at positivity rate of the origin for all direct flights to Detroit.\nFor each airport, we look at the frequency of flights between that airport and DTW and the associated positivity rate in the surrounding state to assign a scaled COVID score to each origin airport.\nThis allows us to show where, from around the country, COVID is coming to Detroit from.\nThen a scale is then used to classify the danger level of each associated vector.\n")
    
    for airport in vectors:
        outFile.write(f"\nCity: {airport[4]}, Airport Code: {airport[0]}, Flight Frequency: {airport[9]}, Positiviy Rate: {airport[10]}, Vector Score: {airport[11]} , Danger level: {airport[12]}")
      
    outFile.close()

def main():
    # Connect to Database
    cur, conn, = setUpDatabase("data.db")

    # Join Corona and Locals table
    cleanAndJoin(cur)

    # Find origin frequency from arrivals and generate Vector Score and Danger Level
    calculateVectorItensity(cur)

    # Output to Text File
    outputVectorsToFile("Output.txt")
    print("Output is now avalible in Output.txt")

    conn.close()

main()