
import pandas as pd
from datetime import datetime

timestamp_label = 'Measurement Timestamp Label'
aggregate_query = {'Air Temperature':['min', 'max']}
column_name_map = {('Air Temperature', 'min'): 'Min Temp', ('Air Temperature', 'max'): 'Max Temp'}
key = ['Station Name', 'Date']

def to_datetime(s):
    return datetime.strptime(s, '%m/%d/%Y %I:%M %p')

def start(group):
    start = group.iloc[0]['Air Temperature']
    start_dt = to_datetime(group.iloc[0][timestamp_label])
    for i, row in group.iterrows():
        dt = to_datetime(row[timestamp_label])
        if dt < start_dt:
            start = row['Air Temperature']
            start_dt = dt
    return start

def end(group):
    end = group.iloc[0]['Air Temperature']
    end_dt = to_datetime(group.iloc[0][timestamp_label])
    for i, row in group.iterrows():
        dt = to_datetime(row[timestamp_label])
        if dt > end_dt:
            end = row['Air Temperature']
            end_dt = dt
    return end

def process_csv(reader, writer):
    weather_data = pd.read_csv(reader)
    dates = []
    for i, row in weather_data.iterrows():
        timestamp = row[timestamp_label]
        date = timestamp.split()[0]
        dates.append(date)
    weather_data['Date'] = dates
    
    groups = weather_data.groupby(key)
    
    aggregates = groups.agg(aggregate_query)
    s = groups.apply(start).rename('First Temp')
    e = groups.apply(end).rename('Last Temp')
    
    aggregates = aggregates.join(s, on=key)
    aggregates = aggregates.join(e, on=key)
    aggregates = aggregates.rename(columns=column_name_map)
    
    aggregates.to_csv(writer)
    return s, e, aggregates