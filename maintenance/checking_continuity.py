import os
import pandas as pd

starting_date = '2022-03-02'
ending_date = '2023-06-22'

dirs = [r'Z:\Data\Twitter\A\unzipped', r'Z:\Data\Twitter\B1\unzipped', r'Z:\Data\Twitter\B2\unzipped', r'Z:\Data\Twitter\C1\unzipped', r'Z:\Data\Twitter\C2\unzipped', r'Z:\Data\Twitter\D\unzipped', r'Z:\Data\Twitter\E\unzipped']
left_out = {}
special = []
for dir in dirs:
    possible_dates = [str(date.date()) for date in pd.date_range(starting_date, ending_date, freq='D')]
    letter = dir.split('\\')[-2]
    for file in os.listdir(dir):
        try:
            possible_date = file.replace('.parquet', '')
            possible_dates.remove(possible_date)
        except:
            special.append(file)
    left_out[letter] = possible_dates

from datetime import datetime, timedelta


def format_date_ranges(dates):
    # Convert string dates to datetime.date objects
    dates = [datetime.strptime(date, '%Y-%m-%d').date() for date in dates]
    dates.sort()

    result = []
    start = dates[0]
    end = dates[0]

    for current in dates[1:]:
        # If the current date is the next day after the end of the current range
        if current == end + timedelta(days=1):
            end = current
        else:
            # If the range is more than one day, append it in the desired format
            if start != end:
                result.append(f"{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}")
            else:
                result.append(start.strftime('%Y-%m-%d'))

            start = end = current

    # Handle the last range or date
    if start != end:
        result.append(f"{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}")
    else:
        result.append(start.strftime('%Y-%m-%d'))

    return result

new_left_out = {}
for key in left_out.keys():
    new_left_out[key] = format_date_ranges(left_out[key])

for key in new_left_out.keys():
    print("=====================================")
    print(key)
    print('-------------------------------------')
    print(new_left_out[key])