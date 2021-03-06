import json
import unittest
import os
import sqlite3
import re
import plotly.express as px
import pandas as pd


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
       
        try:
            posRate = float(flight[6])/flight[7]
            vectorScore = freq * (float(flight[6])/flight[7]) * 100
        except:
            posRate = 0.0
            vectorScore = 10 #fallback for 0 division

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

def get_cities():
    longlist = []
    for vector in vectors:
        longlist.append(vector[4])

    return longlist

def get_flightfreq ():
    freqlist = []
    for vector in vectors:
        freqlist.append(vector[9])
    
    return freqlist

# PURPOSE: Use vectors list to create visual
# INPUT: Vectors list with complete data
# OUTPUT: Visual representation

def createVisuals():
    
    cities = get_cities()
    flightfreq = get_flightfreq()

    df = pd.DataFrame(dict(cities = cities, flightfreq = flightfreq))

    # Use column names of df for the different parameters x, y, color, ...
    fig = px.bar(df, x="cities", y="flightfreq",
                    text = "flightfreq",
                    title="Calculated Flight Frequencies to DTW from other major airports",
                    labels={'flightfreq': "Flight Frequencies"} 
                    )

    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig.update_layout(barmode='group', xaxis_tickangle=-45)

    fig.show()

def main():
    # Connect to Database
    cur, conn, = setUpDatabase("data.db")

    # Join Corona and Locals table
    cleanAndJoin(cur)

    # Find origin frequency from arrivals and generate Vector Score and Danger Level
    calculateVectorItensity(cur)

    # Create Visual
    createVisuals()
    print("Visual Generated")

    conn.close()

main()