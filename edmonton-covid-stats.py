#!/usr/bin/env python3
#
# you need to download the CSV file from
# https://www.alberta.ca/stats/covid-19-alberta-statistics.htm#data-export
# it's a dynamic url =(

import argparse
import csv
from prettytable import PrettyTable
import sqlite3
import sys
from os import path


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


def main():

    parser = argparse.ArgumentParser(description='Edmonton COVID Statistical Tool')
    parser.add_argument('--import', '-i', dest='csvfile', metavar='CSV_FILE', help='CSV file to import')
    parser.add_argument('--list-zones', dest='list_zones', action='store_true', default=False, help='List known zones')
    parser.add_argument('--zone', dest='zone', action='append', help='Constrain results to zone (ie "Edmonton" or "Edmonton Zone")')
    parser.add_argument('--case-status', dest='case_status', action='store_true', default=False, help='List case totals by status')
    parser.add_argument('--case-age', dest='case_age', action='store_true', default=False, help='List case status by age')

    args = parser.parse_args()

    conn = sqlite3.connect('edmonton-covid.db')
    c    = conn.cursor()

    if args.list_zones:
        for zone in zone_lookup(c):
            print(zone)

    zone = None
    if args.zone:
        zone = []
        print(args.zone)
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
            print(zone)
        else:
            zone = []
            for z in zone_lookup(c):
                headers.append(z)
                zone.append(z)

        for z in zone:
            print(z)
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Recovered" and Zone = ?', [z]):
                stats['Recovered'][z] = row[0]
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Active" and Zone = ?', [z]):
                stats['Active'][z] = row[0]
            for row in c.execute('SELECT COUNT(Num) FROM covid where Status = "Died" and Zone = ?', [z]):
                stats['Died'][z] = row[0]
        t = PrettyTable(headers)
        t.align = 'r'
        for rname in stats:
            r = [rname]
            for r1 in stats[rname]:
                r.append('{:,}'.format(stats[rname][r1]))
            t.add_row(r)
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
            print(zone)
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

        t = PrettyTable(header)
        t.align = 'r'
        for rname in case_ages(c):
            r = [rname]
            for z in zone:
                for x in status:
                    r.append('{:,}'.format(stats[x][z][rname]))
            t.add_row(r)
        print(t)

    if args.csvfile:
        if not path.isfile(args.csvfile):
            print(f'{args.csvfile} is not a file to import!')
            sys.exit(1)

        data = []
        with open(args.csvfile) as covid_file:
            csv_reader = csv.reader(covid_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    # ('Num', 'Date reported', 'Alberta Health Services Zone', 'Gender', 'Age group', 'Case status', 'Case type')
                    # use a fixed set, the above is what the edmonton covid export has
                    if len(row) != 7:
                        print(f'Unexpected number of columns in {args.csvfile}; expected 7 got {len(row)}!')
                        sys.exit(1)

                    c.execute('DROP TABLE covid')
                    c.execute('CREATE TABLE covid (Num int, Reported text, Zone text, Gender text, AgeGroup text, Status text, Type text)')
                    line_count += 1
                else:
                    data.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
                    line_count += 1
            c.executemany('INSERT INTO covid VALUES (?,?,?,?,?,?,?)', data)
            print(f'Imported {line_count-1} lines.')
        conn.commit()
        conn.close()


if __name__ == '__main__':
    main()
