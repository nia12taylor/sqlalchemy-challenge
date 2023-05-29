# Import the dependencies.
import datetime as dt
from dateutil.relativedelta import relativedelta
import numpy as np
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///../Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement=Base.classes.measurement
Station = Base.classes.station

# Set labels for temperature data
temp_names=['TMin','TAvg','TMax']

#Function to convert tuples to a list pf dictionaries
def convert_to_list(res, names):
    temp_list=[]
    for row in res:
        temp_dict={}
        i=0
        for i, name in enumerate(names):
            temp_dict[name] = row[i]
    
        temp_list.append(temp_dict)
    return temp_list

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route('/')
def welcome():
    """List all available api routes."""
    return (
        f"<h1>Available Routes:</h1><br/>"
        f'<a href="http://127.0.0.1:5000/api/v1.0/precipitation"><b>/api/v1.0/precipitation</b></a><br/>'
        f'<a href="http://127.0.0.1:5000/api/v1.0/stations"><b>/api/v1.0/stations</b></a><br/>'
        f'<a href="http://127.0.0.1:5000/api/v1.0/tobs"><b>/api/v1.0/tobs</b></a><br/>'
        f'<a href="http://127.0.0.1:5000/api/v1.0/2017-08-01"><b>/api/v1.0/&lt;start_date(YYYY-MM-DD)&gt;</b><br/>'
        f'<a href="http://127.0.0.1:5000/api/v1.0/2016-08-01/2017-01-01"><b>/api/v1.0/&lt;start_date&gt;/&lt;end_date&gt;</b><br/>'
    )


@app.route('/api/v1.0/precipitation')
def gerprecipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    # Design a query to retrieve the last 12 months of precipitation data and plot the results. 
    # Get most recent data point in the database. 
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]

    # Calculate the date one year from the last date in data set.
    most_recent_date_fmt = dt.datetime.strptime(most_recent_date,'%Y-%m-%d')
    year_from_recent_date = (most_recent_date_fmt - relativedelta(years=1)).strftime("%Y-%m-%d")

    # Perform a query to retrieve the data and precipitation scores
    yearly_precipitation = session.query(Measurement.date,Measurement.prcp).\
                        filter(Measurement.date >= year_from_recent_date).\
                        filter(Measurement.date <= most_recent_date).all()
    # Close the DB session
    session.close()

    precipitation_dict = {}
    for date,prcp in yearly_precipitation:
        precipitation_dict[date] = prcp
       
    return jsonify(precipitation_dict)
    

@app.route('/api/v1.0/stations')
def getstations():
    """List of all the stations in the data
    Return: json list of of station Ids.
    """
    # Create our session (link) from Python to the DB
    session = Session(engine)
    stations = session.query(Measurement.station).distinct().all()
    # Close the DB session
    session.close()
    #Create a list from the response
    all_stations = list(np.ravel(stations))
    # convert the list to json and return formatted list.
    return jsonify(all_stations)


@app.route('/api/v1.0/tobs')
def getactivestation_data():
    """List the temperature observations for the last 12 month period for the most active station 
    Return: json list of temperatures observations for last 12 month period for the most active station.
    """
    # Create our session (link) from Python to the DB
    session = Session(engine)
    result = session.execute("select station, max(date) as to_date from measurement group by station order by count(date) desc limit 1;").fetchall()
    active_station = result[0][0]
    active_station_date = result[0][1]
    last_active_date = dt.datetime.strptime(active_station_date,'%Y-%m-%d')
    year_from_last_active_dt= (last_active_date - dt.timedelta(days=365)).strftime("%Y-%m-%d")
    result_temps = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == active_station).\
                                filter(Measurement.date >= year_from_last_active_dt).filter(Measurement.date <= active_station_date).all()
    # Close the DB session
    session.close()
    #Prepare the labels for the response
    names=['date','temperature']
    #Call the function to return a list of dictionaries, convert it to json and return formatted list.
    return jsonify(convert_to_list(result_temps,names))
    

@app.route('/api/v1.0/<start_date>')
def get_temp_start_date(start_date):
    """List the average, minimum, amd maximum temperatures from a start date
    Keyword arguments:
    start_date -- Start date for temperature observations
    Return: json list of average, minimum, and maximum temperatures for observations that are on or after the start date.
    """
    #Format the input date 
    start_date_clean= start_date.replace("%20","-")

    # Create our session (link) from Python to the DB
    session = Session(engine)
    result = session.query(func.min(Measurement.tobs),func.avg(Measurement.tobs),func.max(Measurement.tobs)).\
                            filter(Measurement.date >= start_date_clean).all()
    # Close the DB session
    session.close()
    
    #Call the function to return a list of dictionaries, convert it to json and return formatted list.
    return jsonify(convert_to_list(result,temp_names))


@app.route('/api/v1.0/<start_date>/<end_date>')
def get_temp_start_end_date(start_date,end_date):
    """List the average, minimum, amd maximum temperatures for a time period
    Keyword arguments:
    start_date -- Start date for temperature observations
    end_date -- End date for temperature observations
    Return: json list of average, minimum, and maximum temperatures for the period
    """
    #Format the input date 
    start_date_clean= start_date.replace("%20","-")
    end_date_clean= end_date.replace("%20","-")
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    result = session.query(func.min(Measurement.tobs),func.avg(Measurement.tobs),func.max(Measurement.tobs)).\
                            filter(Measurement.date >= start_date_clean).filter(Measurement.date <= end_date_clean).all()
    
    # Alternate way to query the database session - execute(f"select min(tobs) AS TMIN, avg(tobs) AS TAVG, max(tobs) AS TMAX from measurement where date >= '{start_date_clean}' and date <= '{end_date_clean}';").fetchall()
    
    # Close the DB session
    session.close()
    
    #Call the function to return a list of dictionaries, convert it to json and return formatted list.
    return jsonify(convert_to_list(result,temp_names))


if __name__ == '__main__':
    app.run(debug=True)