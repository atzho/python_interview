from interview import weather
import io
import pandas as pd

'''
Checks if the number of unique date/station pairs is the same as the original
'''
def length_check(reader, df):
    unique = set()
    for _, row in pd.read_csv(reader).iterrows():
        timestamp = row['Measurement Timestamp Label'].split()
        unique.add((row['Station Name'], timestamp[0]))
    return len(df.index)==len(unique)

'''
Checks if the min and max are the min and max for that row
'''
def sanity_check(df):
    for _, row in df.iterrows():
        temps = [row['Max Temp'], row['Min Temp'], row['First Temp'], row['Last Temp']]
        if row['Max Temp'] != max(temps):
            return False
        if row['Min Temp'] != min(temps):
            return False
    return True

'''
Run tests. We call all tests together so that we only have to run
process_csv one time.
'''
def test_suite():
    reader = "data/chicago_beach_weather.csv"
    writer = io.StringIO()
    weather.process_csv(reader, writer)

    writer.seek(0)
    df = pd.read_csv(writer)

    assert length_check(reader, df)
    assert sanity_check(df)