import json
import unittest
import os
import requests
import sqlite3
import re


# GenerateResults.py
# This peice of code combines the database tables and generates a list of COVID vectors from around the country into DTW
''' Data '''
vectors = []

''' FUNCTIONS '''

# PURPOSE: Join Tables

def cleanAndJoin(cur):
    cur.execute("Select Locals.code, Locals.lng, Locals.lat, Locals.state, Locals.cityName, Locals.countryCode, Corona.peoplePositiveNewCasesCt, Corona.peopleNegativeNewCt, Corona.peopleDeathCt from Locals join Corona on Locals.state = Corona.state")
    for peice in cur:
        vectors.append(list(peice))
        

def calculateVectorItensity(cur):

    for flight in vectors:
        cur.execute("SELECT * FROM Flights WHERE origin=?", (flight[0],))
        freq = len(cur.fetchall())
        posRate = float(flight[6])/flight[7]
        vectorScore = freq * (float(flight[6])/flight[7]) * 100
        flight.append(freq)
        flight.append(posRate)
        flight.append(vectorScore)
        # Generate Warning Label


def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def main():
    cur, conn, = setUpDatabase("data.db")
    cleanAndJoin(cur)
    calculateVectorItensity(cur)
    print(vectors)
    conn.close

main()