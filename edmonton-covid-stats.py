#!/usr/bin/env python3
#
# you need to download the CSV file from
# https://www.alberta.ca/stats/covid-19-alberta-statistics.htm#data-export
# it's a dynamic url =(

import argparse
import configparser
import csv
import datetime
from io import StringIO
import pandas
from gspread_pandas import Spread, Client
from prettytable import PrettyTable
import sqlite3
import sys
from os import path, environ

max_weeks = 53

def zone_lookup(c):
    zones = []
    for row in c.execute('SELECT DISTINCT Zone from covid'):
        zones.append(row[0])
    return zones


def case_ages(c):
    ages = []
    for row in c.execute('SELECT DISTINCT AgeGroup FROM covid'):
        ages.append(row[0])
    return sorted(ages)


def get_year_week(date_str):
    d = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    (year, week, weekday) = d.isocalendar()
    return (year, week)

def main():

    parser = argparse.ArgumentParser(description='Edmonton COVID Statistical Tool')
    parser.add_argument('--import', '-i', dest='csvfile', metavar='CSV_FILE', help='CSV file to import')
    parser.add_argument('--list-zones', dest='list_zones', action='store_true', default=False, help='List known zones')
    parser.add_argument('--zone', dest='zone', action='append', help='Constrain results to zone (ie "Edmonton" or "Edmonton Zone")')
    parser.add_argument('--case-status', dest='case_status', action='store_true', default=False, help='List case totals by status')
    parser.add_argument('--case-age', dest='case_age', action='store_true', default=False, help='List case status by age')
    parser.add_argument('--case-detected', dest='case_detected', action='store_true', default=False, help='List cases by week detected')
    parser.add_argument('--csv', dest='csv', action='store_true', default=False, help='Output CSV rather than a table')
    parser.add_argument('--config', dest='config', metavar='CONFIG_FILE', help='Configuration file, default is $HOME/.gsheet.ini')

    args = parser.parse_args()

    config = configparser.ConfigParser()

    if args.config:
        config.read(args.config)
    else:
        config.read(environ['HOME'] + '/.gsheet.ini')

    sheet_update = True
    try:
        token_user = config.get('covid', 'token_user')
        sheet_book = config.get('covid', 'sheet_book')
    except:
        sheet_update = False

    conn = sqlite3.connect('edmonton-covid.db')
    c    = conn.cursor()

    if args.list_zones:
        for zone in zone_lookup(c):
            print(zone)

    zone = None
    if args.zone:
        zone = []
        for z in args.zone:
            z = z.title()
            if 'Zone' not in z:
                if z == 'Unknown':
                    zone.append(z)
                else:
                    zone.append(f'{z} Zone')
            else:
                zone.append(z)

        for z in zone:
            if z not in zone_lookup(c):
                print(f'Zone "{z}" is not a valid zone, use --list-zones for a list!')
                sys.exit(1)

        if not args.csv:
            print(f'Constraining results to zone(s): {", ".join(zone)}')

    if args.case_status:
        headers = ['Status', 'All']
        stats = {'Recovered': {}, 'Active': {}, 'Died': {}}
        for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Recovered"'):
            stats['Recovered']['all'] = row[0]
        for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Active"'):
            stats['Active']['all'] = row[0]
        for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Died"'):
            stats['Died']['all'] = row[0]
        if zone:
            for z in zone:
                headers.append(z)
        else:
            zone = []
            for z in zone_lookup(c):
                headers.append(z)
                zone.append(z)

        for z in zone:
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Recovered" and Zone = ?', [z]):
                stats['Recovered'][z] = row[0]
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Active" and Zone = ?', [z]):
                stats['Active'][z] = row[0]
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Died" and Zone = ?', [z]):
                stats['Died'][z] = row[0]
        if args.csv:
            print(','.join(headers))
        else:
            t = PrettyTable(headers)
            t.align = 'r'

        for rname in stats:
            r = [rname]
            for r1 in stats[rname]:
                if args.csv:
                    r.append(stats[rname][r1])
                else:
                    r.append('{:,}'.format(stats[rname][r1]))
            if args.csv:
                print(','.join(str(a) for a in r))
            else:
                t.add_row(r)
        if not args.csv:
            print(t)

    if args.case_age:
        status = ['Recovered', 'Active', 'Died']
        header = ['Case Age']
        stats = {'Recovered': {'all': {}}, 'Active': {'all': {}}, 'Died': {'all': {}}}
        for age in case_ages(c):
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Recovered" and AgeGroup = ?', [age]):
                stats['Recovered']['all'][age] = row[0]
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Active" and AgeGroup = ?', [age]):
                stats['Active']['all'][age] = row[0]
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Died" and AgeGroup = ?', [age]):
                stats['Died']['all'][age] = row[0]
        if zone:
            for z in zone:
                if 'Zone' in z:
                    zt = z.split(' ')[0]
                else:
                    zt = z
                for s in status:
                    header.append(f'{zt}-{s}')
        else:
            zone = []
            for z in zone_lookup(c):
                if 'Zone' in z:
                    zt = z.split(' ')[0]
                else:
                    zt = z
                for s in status:
                    header.append(f'{zt}-{s}')
                zone.append(z)

        for z in zone:
            for x in status:
                stats[x][z] = {}
            for age in case_ages(c):
                for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Recovered" and Zone = ? and AgeGroup = ?', [z, age]):
                    stats['Recovered'][z][age] = row[0]
                for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Active" and Zone = ? and AgeGroup = ?', [z, age]):
                    stats['Active'][z][age] = row[0]
                for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Died" and Zone = ? and AgeGroup = ?', [z, age]):
                    stats['Died'][z][age] = row[0]

        if args.csv:
            print(','.join(header))
        else:
            t = PrettyTable(header)
            t.align = 'r'

        for rname in case_ages(c):
            r = [rname]
            for z in zone:
                for x in status:
                    if args.csv:
                        r.append(stats[x][z][rname])
                    else:
                        r.append('{:,}'.format(stats[x][z][rname]))
            if args.csv:
                print(','.join(str(a) for a in r))
            else:
                t.add_row(r)
        if not args.csv:
            print(t)

    if args.case_detected:
        if not args.zone:
            print('Use --zone to isolate to a particular zone')
            sys.exit(1)

        for z in zone:
            header = ['Case Detected Week Of']
            for age in case_ages(c):
                header.append(age)
            header.append('Total')

            stats = {z: {}}

            if args.csv:
                print(','.join(header))
            for year in [2020, 2021]:
                stats[z][year] = {}
                week = 1
                while week < max_weeks:
                    stats[z][year][week] = {}
                    for age in case_ages(c):
                        stats[z][year][week][age] = 0
                        for row in c.execute('SELECT COUNT(Num) FROM covid where AgeGroup = ? and WeekNum = ? and Zone = ? and Reported LIKE ?', [age, week, z, str(year)+'-%']):
                            stats[z][year][week][age] = row[0]
                    week += 1

            # TODO: for some reason, 2021 also includes 2020 and it's not supposed to...
            print(f'Detected cases for zone: {z}')
            for year in [2020, 2021]:
                t = PrettyTable(header)
                t.align = 'r'
                if not args.csv:
                    print(f'{year}:')
                week = 1
                while week < max_weeks:
                    row = []
                    first_of_week = datetime.datetime.fromisocalendar(year, week, 1)
                    week_start = first_of_week.strftime('%Y-%m-%d')
                    row.append(week_start)
                    total = 0
                    for age in stats[z][year][week].keys():
                        row.append(stats[z][year][week][age])
                        total += stats[z][year][week][age]
                        #print(f'{week_start}: {age}: {stats[z][year][week][age]}')
                    row.append(total)

                    # only include if the total > 0
                    if total > 0:
                        if args.csv:
                            print(','.join(str(a) for a in row))
                        else:
                            t.add_row(row)
                    week += 1
                if not args.csv:
                    print(t)
                    print('')
            #print(stats[z])


    if args.csvfile:
        if not path.isfile(args.csvfile):
            print(f'{args.csvfile} is not a file to import!')
            sys.exit(1)

        data       = []
        csv_header = 'Num,Date reported,Alberta Health Services Zone,Gender,Age group,Case status,Case type\n'
        csv_years  = {2020: csv_header, 2021: csv_header}

        with open(args.csvfile) as covid_file:
            csv_reader = csv.reader(covid_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    # ('Date reported', 'Alberta Health Services Zone', 'Gender', 'Age group', 'Case status', 'Case type')
                    # use a fixed set, the above is what the Alberta covid export has
                    if len(row) != 6:
                        print(f'Unexpected number of columns in {args.csvfile}; expected 7 got {len(row)}!')
                        sys.exit(1)

                    c.execute('DROP TABLE covid')
                    c.execute('CREATE TABLE covid (Num int, Reported text, WeekNum int, Zone text, Gender text, AgeGroup text, Status text, Type text)')
                    line_count += 1
                else:
                    (year, weeknum) = get_year_week(row[0])
                    data.append((line_count, row[0], weeknum, row[1], row[2], row[3], row[4], row[5]))
                    csv_years[year] += f'{line_count},{row[0]},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]}\n'
                    line_count += 1
            c.executemany('INSERT INTO covid VALUES (?,?,?,?,?,?,?,?)', data)
            print(f'Imported {line_count-1} lines.')
        conn.commit()
        conn.close()

        if sheet_update:
            for csv_year in csv_years:
                sname  = f'PIVOT-{csv_year}'
                df     = pandas.read_csv(StringIO(csv_years[csv_year]))
                spread = Spread(sheet_book)
                spread.df_to_sheet(df, index=False, sheet=sname,start='A1', replace=True)
                spread.freeze(rows=1, sheet=sname)
            print(f'Updated spreadsheet: {sheet_book}')


if __name__ == '__main__':
    main()
