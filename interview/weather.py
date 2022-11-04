
import pandas as pd
from datetime import datetime

'''
Constants
'''
timestamp_label = 'Measurement Timestamp Label'
aggregate_query = {'Air Temperature':['min', 'max']}
column_name_map = {('Air Temperature', 'min'): 'Min Temp', ('Air Temperature', 'max'): 'Max Temp'}
key = ['Station Name', 'Date']

'''
Parses timestamps based on formats
'''
def to_datetime(s):
    return datetime.strptime(s, '%m/%d/%Y %I:%M %p')

'''
Custom aggregation function that finds the latest temperature
in a given group. Returns a scalar value to be broadcast.
'''
def start(group):
    start = group.iloc[0]['Air Temperature']
    start_dt = to_datetime(group.iloc[0][timestamp_label])
    for i, row in group.iterrows():
        dt = to_datetime(row[timestamp_label])
        if dt < start_dt:
            start = row['Air Temperature']
            start_dt = dt
    return start

'''
Custom aggregation function that finds the latest temperature
in a given group. Returns a scalar value to be broadcast.
'''
def end(group):
    end = group.iloc[0]['Air Temperature']
    end_dt = to_datetime(group.iloc[0][timestamp_label])
    for _, row in group.iterrows():
        dt = to_datetime(row[timestamp_label])
        if dt > end_dt:
            end = row['Air Temperature']
            end_dt = dt
    return end

'''
process_csv computes the following daily aggregates from weather data:
- Max Temp
- Min Temp
- First Temp
- Last Temp

args:
reader: filename, file, or IO buffer (any pandas.read_csv compatible input)
writer: filename, file, or IO buffer (any pandas.dataframe.to_csv compatible input)

returns:
weather_data: original dataframe from reader
aggregates: table of min/max aggregates, grouped by date and station
starts: table of start aggregates, grouped by date and station
ends: table of end aggregates, grouped by date and station
'''
def process_csv(reader, writer):
    weather_data = pd.read_csv(reader)

    # Extract dates from timestamps
    dates = []
    for _, row in weather_data.iterrows():
        timestamp = row[timestamp_label]
        date = timestamp.split()[0]
        dates.append(date)
    weather_data['Date'] = dates
    
    # Group data keyed by date and station
    groups = weather_data.groupby(key)
    
    # Compute aggregates. For min, max we use built-in function. For
    # start, end we use our custom aggregation functions.
    aggregates = groups.agg(aggregate_query)
    starts = groups.apply(start).rename('First Temp')
    ends = groups.apply(end).rename('Last Temp')
    
    # Add start and end temperatures by JOIN and rename columns
    aggregates = aggregates.join(starts, on=key)
    aggregates = aggregates.join(ends, on=key)
    aggregates = aggregates.rename(columns=column_name_map)
    
    aggregates.to_csv(writer)
    
    # Testing output
    return weather_data, aggregates, starts, ends