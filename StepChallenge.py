#!/usr/bin/env python
# coding: utf-8
"""
Process Google Forms results into useful data products.

First, create a corrected snapshot of the data.
Names are stripped on extra spaces, and capitalization is cannonicised.

Then, sub-sheets are created for each Peaker group represented.
"""

import argparse
import collections
import os
import pickle
import sys
import time

# Needed tools:
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

# The Link to the spreadsheet to read data from.
SOURCE_SPREADSHEET = '1spfXPQgzg3UTTPtdKMelvbwYWVHuNFD4FWXdU5rc8vI'
# The link to the corrected data spreadsheet
CORRECTED_SPREADSHEET = '1SYjohmGJklzQP5pQXdWKawK26HLavKxruoPs4L8kMe0'


class Sheets:
    """
    Methods to talk to Google Sheets
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    def __init__(self):
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        print("Connecting...")
        store = file.Storage('token.json')
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
            creds = tools.run_flow(flow, store)

        service = build('sheets', 'v4', http=creds.authorize(Http()))

        # Call the Sheets API
        self.sheet = service.spreadsheets()
        print("...connected")

    def get_data(self, source, range):
        return self.sheet.values().get(spreadsheetId=source, range=range).execute().get('values', [])

    def write_data(self, dest, data):
        columns = len(data[0])
        request = self.sheet.values().update(
            spreadsheetId=dest, range='Sheet1!A:{}'.format(chr(ord('A') + columns - 1)),
            valueInputOption="USER_ENTERED",
            body={'values': data})
        resp = request.execute()
        time.sleep(3.0)
    
    def write_cell(self, dest, cell, data):
        request = self.sheet.values().update(
            spreadsheetId=dest, range='Sheet1!' + cell, valueInputOption="USER_ENTERED",
            body={'values': [[data]]})
        resp = request.execute()
        time.sleep(2.0)

    def make_new_sheet(self, name):
        spreadsheet_body = {'properties': {'title': name}}
        request = self.sheet.create(body=spreadsheet_body)
        response = request.execute()
        print("New Sheet {}: {}".format(name, response['spreadsheetUrl']))
        return response



class GroupSplitter:
    PickleFile = 'group_sheets.pickle'
    def __init__(self, groups):
        self.group_sheets = {}
        self.desired_groups = groups
        if os.path.exists(GroupSplitter.PickleFile):
            with open(GroupSplitter.PickleFile, 'rb') as f:
                self.group_sheets = pickle.load(f)

    def save_groups(self):
        with open(GroupSplitter.PickleFile, 'wb') as f:
            pickle.dump(self.group_sheets, f)

    def split(self, sheets, data, time_of_data):
        def header_line():
            return [data[0]]

        group_data = collections.defaultdict(header_line)

        print("sorting...")
        for entry in data[1:]:
            group = entry[2]
            if not group in self.group_sheets:
                self.group_sheets[group] = sheets.make_new_sheet(group)
            dest = self.group_sheets[group]
            group_data[group].append(entry)
        print("...sorted")

        count = 1
        max_count = len(self.desired_groups) if self.desired_groups else len(group_data)
        for group in group_data:
            # Add a blank line to make
            group_data[group].append(['','','','','',''])
            if not self.desired_groups or group in self.desired_groups:
                ret = sheets.write_data(self.group_sheets[group]['spreadsheetId'], group_data[group])
                sheets.write_cell(self.group_sheets[group]['spreadsheetId'], 'H1', time_of_data)
                print("wrote {} ({}/{})".format(group, count, max_count))
                count += 1



def canonicise_name(n):
    # First, strip spaces.
    n = n.strip()
    # Split into words
    words = n.split()
    # Return each word capitalized, separated by one space.
    return ' '.join([x.capitalize() for x in words])


def sheet_to_alias_table(alias_data):
    ret = {}
    for entry in alias_data:
        if entry and entry[0]:
            ret[canonicise_name(entry[0])] = entry[1]
    return ret


def correct_data(raw_data, aliases, count=None):
    if count is None:
        count = len(raw_data)
    ret = []
    already_seen = set()
    for index in range(count):
        corrected_entry = raw_data[index]
        # Check for Dups BEFORE canonicalization.
        # If they differ, they aren't the dup.
        sans_timestamp = ','.join(corrected_entry[1:])
        if sans_timestamp in already_seen:
            continue
        already_seen.add(sans_timestamp)
        name = canonicise_name(corrected_entry[1])
        # Deal with aliases
        corrected_entry[1] = aliases.get(name, name)
        ret.append(corrected_entry)
    return ret


if __name__ == '__main__':
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('-c', '--count', type=int, help='How many rows to process')
    parser.add_argument('-g', '--group', action='append', dest='groups', default=[])

    options = parser.parse_args()

    sheets = Sheets()
    raw_peaker_data = sheets.get_data(SOURCE_SPREADSHEET, "Form Responses 1!A:F")
    time_of_data = time.asctime()
    print("Got data at {}".format(time_of_data))
    alias_table = sheet_to_alias_table(sheets.get_data(SOURCE_SPREADSHEET, "Aliases!A:B"))
    
    corrected_data = correct_data(raw_peaker_data, alias_table, options.count)
    print("Corrected {} lines of data".format(len(corrected_data)))

    sheets.write_data(CORRECTED_SPREADSHEET, corrected_data)
    sheets.write_cell(CORRECTED_SPREADSHEET, 'H1', time_of_data)

    splitter = GroupSplitter(options.groups)
    splitter.split(sheets, corrected_data, time_of_data)
    splitter.save_groups()

    print()
    print("Finished writing data for {} entries".format(len(corrected_data)))