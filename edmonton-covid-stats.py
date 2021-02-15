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
from gspread_pandas import Spread
from prettytable import PrettyTable
import sqlite3
import sys
from os import path, environ

MAX_WEEKS  = 53
MAX_MONTHS = 13
ALL_YEARS  = [2020, 2021]
# as per https://www.alberta.ca/population-statistics.aspx
AB_POP     = 4428112


def output_make_headers(headers):
    icvs = '%s\n' % ','.join(headers)
    t = PrettyTable(headers)
    t.align = 'r'

    return icvs, t


def output_add_row(icvs, t, r):
    icvs += '%s\n' % ','.join(str(a) for a in r)
    t.add_row(r)

    return icvs, t


def zone_lookup(c):
    zones = []
    for row in c.execute('SELECT DISTINCT Zone from covid'):
        zones.append(row[0])

    return zones


def get_zones(zones, c):
    zone = []
    for z in zones:
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

    return zone


def case_ages(c):
    ages = []
    for row in c.execute('SELECT DISTINCT AgeGroup FROM covid'):
        ages.append(row[0])

    return sorted(ages)


def get_year_week(date_str):
    d = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    (year, week, weekday) = d.isocalendar()

    return year, week


def do_case_status(zone, print_csv, c):
    # Function to print current case status
    #
    # it will return the CSV list for whatever the caller wants to do with it if
    # args.csv is True, otherwise it will print to stdout

    headers = ['Status', 'All']
    stats   = {'Recovered': {}, 'Active': {}, 'Died': {}, 'Total': {'all': 0}}

    for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Recovered"'):
        stats['Recovered']['all'] = row[0]
        stats['Total']['all'] += row[0]
    for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Active"'):
        stats['Active']['all'] = row[0]
        stats['Total']['all'] += row[0]
    for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Died"'):
        stats['Died']['all'] = row[0]
        stats['Total']['all'] += row[0]
    if zone:
        for z in zone:
            headers.append(z)
    else:
        zone = []
        for z in zone_lookup(c):
            headers.append(z)
            zone.append(z)

    for z in zone:
        stats['Total'][z] = 0
        for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Recovered" and Zone = ?', [z]):
            stats['Recovered'][z] = row[0]
            stats['Total'][z] += row[0]
        for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Active" and Zone = ?', [z]):
            stats['Active'][z] = row[0]
            stats['Total'][z] += row[0]
        for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Died" and Zone = ?', [z]):
            stats['Died'][z] = row[0]
            stats['Total'][z] += row[0]

    (csv, t) = output_make_headers(headers)

    for rname in stats:
        r = [rname]
        for r1 in stats[rname]:
            if print_csv:
                r.append(stats[rname][r1])
            else:
                r.append('{:,}'.format(stats[rname][r1]))
        (csv, t) = output_add_row(csv, t, r)

    if not print_csv:
        print(t)

        p_infected  = (stats['Total']['all'] / AB_POP) * 100
        p_recovered = (stats['Recovered']['all'] / stats['Total']['all']) * 100
        p_died      = (stats['Died']['all'] / stats['Total']['all']) * 100
        p_died_all  = (stats['Died']['all'] / AB_POP) * 100
        p_active    = (stats['Active']['all'] / AB_POP) * 100

        print(f'\nGiven an Alberta population of {AB_POP:,}')
        print(f'  {p_infected:.2f}% of the population was infected')
        print(f'  {p_active:.2f}% of the population is currently infected')
        print(f'  {p_died_all:.2f}% of the population died')
        print(f'  {p_died:.2f}% of those infected died')
        print(f'  {p_recovered:.2f}% of those infected have recovered')

    csv += '\n'
    return csv


def do_case_age(zone, print_csv, c):
    status  = ['Total', 'Recovered', 'Active', 'Died']
    headers = ['Case Age']
    stats   = {'Recovered': {'all': {}}, 'Active': {'all': {}}, 'Died': {'all': {}}, 'Total': {'all': {}}}

    #for age in case_ages(c):
    #    for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Recovered" and AgeGroup = ?', [age]):
    #        stats['Recovered']['all'][age] = row[0]
    #    for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Active" and AgeGroup = ?', [age]):
    #        stats['Active']['all'][age] = row[0]
    #    for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Died" and AgeGroup = ?', [age]):
    #        stats['Died']['all'][age] = row[0]

    if zone:
        if len(zone) > 1:
            for z in zone:
                if 'Zone' in z:
                    zt = z.split(' ')[0]
                else:
                    zt = z
                for s in status:
                    headers.append(f'{zt}-{s}')
        else:
            for s in status:
                headers.append(s)

        for z in zone:
            for x in status:
                stats[x][z] = {}
            for age in case_ages(c):
                for row in c.execute(
                        'SELECT COUNT(Num) FROM covid where Status = "Recovered" and Zone = ? and AgeGroup = ?', [z, age]):
                    stats['Recovered'][z][age] = row[0]
                    stats['Total'][z][age]     = row[0]
                for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Active" and Zone = ? and AgeGroup = ?',
                                     [z, age]):
                    stats['Active'][z][age] = row[0]
                    stats['Total'][z][age] += row[0]
                for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Died" and Zone = ? and AgeGroup = ?',
                                     [z, age]):
                    stats['Died'][z][age]   = row[0]
                    stats['Total'][z][age] += row[0]
    else:
        zone = ['all']
        for s in status:
            headers.append(s)

        for age in case_ages(c):
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Recovered" and AgeGroup = ?', [age]):
                stats['Recovered']['all'][age] = row[0]
                stats['Total']['all'][age]     = row[0]
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Active" and AgeGroup = ?', [age]):
                stats['Active']['all'][age] = row[0]
                stats['Total']['all'][age] += row[0]
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Died" and AgeGroup = ?', [age]):
                stats['Died']['all'][age]   = row[0]
                stats['Total']['all'][age] += row[0]

    (csv, t) = output_make_headers(headers)

    for rname in case_ages(c):
        r = [rname]
        for z in zone:
            for x in status:
                if print_csv:
                    r.append(stats[x][z][rname])
                else:
                    r.append('{:,}'.format(stats[x][z][rname]))
        (csv, t) = output_add_row(csv, t, r)

    if not print_csv:
        print(t)

    csv += '\n'
    return csv


def main():

    parser = argparse.ArgumentParser(description='Edmonton COVID Statistical Tool')
    parser.add_argument('--import', '-i', dest='csvfile', metavar='CSV_FILE', help='CSV file to import')
    parser.add_argument('--list-zones', dest='list_zones', action='store_true', default=False, help='List known zones')
    parser.add_argument('--zone', dest='zone', action='append',
                        help='Constrain results to zone (ie "Edmonton" or "Edmonton Zone")')
    parser.add_argument('--case-status', dest='case_status', action='store_true', default=False,
                        help='List case totals by status')
    parser.add_argument('--case-age', dest='case_age', action='store_true', default=False,
                        help='List case status by age')
    parser.add_argument('--case-detected-weeks', dest='case_detected_weeks', action='store_true', default=False,
                        help='List cases by week detected')
    parser.add_argument('--case-detected-months', dest='case_detected_months', action='store_true', default=False,
                        help='List cases by month detected')
    parser.add_argument('--csv', dest='csv', action='store_true', default=False, help='Output CSV rather than a table')
    parser.add_argument('--config', dest='config', metavar='CONFIG_FILE',
                        help='Configuration file, default is $HOME/.gsheet.ini')

    args   = parser.parse_args()
    config = configparser.ConfigParser()

    csv_output = ''

    if args.config:
        config.read(args.config)
    else:
        config.read(environ['HOME'] + '/.gsheet.ini')

    try:
        sheet_update = True
        sheet_book = config.get('covid', 'sheet_book')
    except:
        sheet_update = False
        sheet_book = None

    conn = sqlite3.connect('edmonton-covid.db')
    c    = conn.cursor()

    if args.list_zones:
        for zone in zone_lookup(c):
            print(zone)

    zone = None
    if args.zone:
        zone = get_zones(args.zone, c)
        if not args.csv:
            print(f'Constraining results to zone(s): {", ".join(zone)}')

    if args.case_status:
        csv_output += do_case_status(zone, args.csv, c)

    if args.case_age:
        csv_output += do_case_age(zone, args.csv, c)

    # catch all, print any CSV output

    if args.csv:
        print(csv_output)

    if args.case_detected_weeks:
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
            for year in ALL_YEARS:
                stats[z][year] = {}
                week = 1
                while week < MAX_WEEKS:
                    stats[z][year][week] = {}
                    for age in case_ages(c):
                        stats[z][year][week][age] = 0
                        for row in c.execute('SELECT COUNT(Num) FROM covid where AgeGroup = ? and WeekNum = ? and Zone = ? and Reported LIKE ?', [age, week, z, str(year)+'-%']):
                            stats[z][year][week][age] = row[0]
                    week += 1

            # TODO: for some reason, 2021 also includes 2020 and it's not supposed to...
            if not args.csv:
                print(f'Detected cases for zone: {z}')

            for year in ALL_YEARS:
                t = PrettyTable(header)
                t.align = 'r'
                if not args.csv:
                    print(f'{year}:')
                week = 1
                while week < MAX_WEEKS:
                    row = []
                    first_of_week = datetime.datetime.fromisocalendar(year, week, 1)
                    week_start = first_of_week.strftime('%Y-%m-%d')
                    row.append(week_start)
                    total = 0
                    for age in stats[z][year][week].keys():
                        row.append(stats[z][year][week][age])
                        total += stats[z][year][week][age]
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

    if args.case_detected_months:
        if not args.zone:
            print('Use --zone to isolate to a particular zone')
            sys.exit(1)

        for z in zone:
            header = ['Case Detected Month Of']
            for age in case_ages(c):
                header.append(age)
            header.append('Total')

            stats = {z: {}}

            if args.csv:
                print(','.join(header))
            for year in ALL_YEARS:
                stats[z][year] = {}
                month = 1
                while month < MAX_MONTHS:
                    stats[z][year][month] = {}
                    for age in case_ages(c):
                        stats[z][year][month][age] = 0
                        mts = f'{year}-{month:02}-%'
                        for row in c.execute('SELECT COUNT(Num) FROM covid where AgeGroup = ? and Zone = ? and Reported LIKE ?', [age, z, mts]):
                            stats[z][year][month][age] = row[0]
                    month += 1

            # TODO: for some reason, 2021 also includes 2020 and it's not supposed to...
            if not args.csv:
                print(f'Detected cases for zone: {z}')

            for year in ALL_YEARS:
                t = PrettyTable(header)
                t.align = 'r'
                if not args.csv:
                    print(f'{year}:')
                month = 1

                while month < MAX_MONTHS:
                    row = []
                    row.append(f'{year}-{month:02}')
                    total = 0
                    for age in stats[z][year][month].keys():
                        row.append(stats[z][year][month][age])
                        total += stats[z][year][month][age]
                    row.append(total)

                    # only include if the total > 0
                    if total > 0:
                        if args.csv:
                            print(','.join(str(a) for a in row))
                        else:
                            t.add_row(row)
                    month += 1
                if not args.csv:
                    print(t)
                    print('')

    #
    # import new data and update SQLite db and possibly a google sheet
    #
    if args.csvfile:
        if not path.isfile(args.csvfile):
            print(f'{args.csvfile} is not a file to import!')
            sys.exit(1)

        data       = []
        csv_header = 'Num,Date reported,Alberta Health Services Zone,Gender,Age group,Case status,Case type\n'
        csv_years  = {}
        for y in ALL_YEARS:
            csv_years[y] = csv_header

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
                spread.df_to_sheet(df, index=False, sheet=sname, start='A1', replace=True)
                spread.freeze(rows=1, sheet=sname)
            print(f'Updated spreadsheet: {sheet_book}')


if __name__ == '__main__':
    main()
