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




def get_longitudes():
    longlist = []
    for vector in vectors:
        longlist.append(vector[4])

    print (longlist)

def get_flightfreq ():
    freqlist = []
    for vector in vectors:
        freqlist.append(vector[9])
    
    print(freqlist)

def get_levels():
    level_list = []
    for vector in vectors:
        level_list.append(vector[12])
    
    print(level_list)

def get_vectorscore():
    vector_score = []
    for vector in vectors:
        vector_score.append(vector[10])

    print(vector_score)



#create visual

cities = ['Madison', 'Austin', 'Windsor Locks', 'Boston', 'Salt Lake City', 'Orlando', 'Los Angeles', 'Louisville', 'Harrisburg', 'Fort Lauderdale', 'Albany', 'Chicago', 'Denver', 'Appleton', 'Tampa', 'Seattle', 'Newark', 'Atlanta', 'Las Vegas', 'Philadelphia', 'New York', 'Dallas-Fort Worth', 'New York', 'Nashville', 'Washington', 'Houston', 'Kansas City', 'Iron Mountain Kingsford', 'Raleigh/Durham', 'Syracuse', 'St Louis', 'Charleston', 'San Francisco', 'Phoenix', 'Charlotte', 'Fort Myers', 'Hebron', 'Rochester', 'Traverse City', 'Bloomington/Normal', 'Portland', 'Green Bay', 'Memphis', 'Buffalo', 'San Diego', 'Burlington', 'Miami', 'Milwaukee', 'Omaha', 'Columbus', 'Indianapolis', 'Alpena', 'San Antonio', 'Pittsburgh', 'Portland', 'Saginaw', 'Grand Rapids', 'Cleveland', 'Lansing', 'Kalamazoo', 'Knoxville', 'Allentown', 'La Crosse', 'Lexington', 'Pellston', 'Elmira/Corning', 'State College', 'Ithaca', 'Dayton', 'Binghamton', 'Marquette', 'Chicago']
posrate = [62.306943091693334, 14.511513009248567, 7.450161531794184, 26.999283838624972, 111.05646939121587, 78.20096874428806, 16.687446775476023, 27.328915034626554, 79.64131725301506, 104.26795832571743, 5.243050916234901, 49.564338931683295, 144.29364589759408, 62.306943091693334, 78.20096874428806, 26.50727650727651, 16.006314820093408, 77.19808025074276, 3743.946188340807, 238.92395175904522, 5.243050916234901, 87.0690780554914, 10.486101832469801, 45.987381703470035, 11.130210571551354, 29.023026018497134, 70.16912558474272, 10.6708647080022, 16.193462480134016, 5.243050916234901, 70.16912558474272, 10.417892878163626, 33.374893550952045, 126.29008922112371, 24.290193720201025, 78.20096874428806, 27.328915034626554, 5.243050916234901, 10.6708647080022, 9.912867786336658, 3.4188881181250617, 62.306943091693334, 30.658254468980022, 5.243050916234901, 16.687446775476023, 6.971677559912854, 52.133979162858715, 62.306943091693334, 52.17146080090242, 18.75475250751349, 129.50804928500764, 10.6708647080022, 14.511513009248567, 79.64131725301506, 10, 10.6708647080022, 10.6708647080022, 18.75475250751349, 10.6708647080022, 10.6708647080022, 15.329127234490011, 79.64131725301506, 62.306943091693334, 27.328915034626554, 10.6708647080022, 5.243050916234901, 79.64131725301506, 5.243050916234901, 18.75475250751349, 5.243050916234901, 10.6708647080022, 9.912867786336658]
level = ['MED', 'LOW', 'LOW', 'MED', 'HIGH', 'MED', 'LOW', 'MED', 'MED', 'HIGH', 'LOW', 'MED', 'HIGH', 'MED', 'MED', 'MED', 'LOW', 'MED', 'VERY HIGH', 'VERY HIGH', 'LOW', 'MED', 'LOW', 'MED', 'LOW', 'MED', 'MED', 'LOW', 'LOW', 'LOW', 'MED', 'LOW', 'MED', 'HIGH', 'MED', 'MED', 'MED', 'LOW', 'LOW', 'LOW', 'LOW', 'MED', 'MED', 'LOW', 'LOW', 'LOW', 'MED', 'MED', 'MED', 'LOW', 'HIGH', 'LOW', 'LOW', 'MED', 'LOW', 'LOW', 'LOW', 'LOW', 'LOW', 'LOW', 'LOW', 'MED', 'MED', 'MED', 'LOW', 'LOW', 'MED', 'LOW', 'LOW', 'LOW', 'LOW', 'LOW']
score = [0.6230694309169333, 0.14511513009248567, 0.07450161531794185, 0.2699928383862497, 0.37018823130405293, 0.26066989581429356, 0.16687446775476023, 0.27328915034626555, 0.7964131725301506, 0.26066989581429356, 0.052430509162349, 0.09912867786336658, 0.4809788196586469, 0.6230694309169333, 0.26066989581429356, 0.08835758835758836, 0.08003157410046703, 0.15439616050148552, 12.47982062780269, 0.7964131725301506, 0.052430509162349, 0.14511513009248567, 0.052430509162349, 0.15329127234490011, 0.05565105285775677, 0.14511513009248567, 0.7016912558474271, 0.106708647080022, 0.08096731240067008, 0.052430509162349, 0.7016912558474271, 0.10417892878163626, 0.16687446775476023, 0.42096696407041234, 0.08096731240067008, 0.26066989581429356, 0.27328915034626555, 0.052430509162349, 0.106708647080022, 0.09912867786336658, 0.034188881181250616, 0.6230694309169333, 0.15329127234490011, 0.052430509162349, 0.16687446775476023, 0.06971677559912855, 0.26066989581429356, 0.6230694309169333, 0.5217146080090242, 0.1875475250751349, 0.6475402464250383, 0.106708647080022, 0.14511513009248567, 0.7964131725301506, 0.0, 0.106708647080022, 0.106708647080022, 0.1875475250751349, 0.106708647080022, 0.106708647080022, 0.15329127234490011, 0.7964131725301506, 0.6230694309169333, 0.27328915034626555, 0.106708647080022, 0.052430509162349, 0.7964131725301506, 0.052430509162349, 0.1875475250751349, 0.052430509162349, 0.106708647080022, 0.09912867786336658]
df = pd.DataFrame(dict(cities = cities, posrate = posrate, level = level, score = score))

# Use column names of df for the different parameters x, y, color, ...
fig = px.scatter(df, x="cities", y="posrate",
                color = "level",
                size = "score",
                 title="Calculated COVID-19 Vector Scores",
                 labels={"COVID positivity rates per city"} 
                )

fig.update_layout(barmode='group', xaxis_tickangle=-45)

fig.show()






    

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

    get_vectorscore()


    conn.close()

main()