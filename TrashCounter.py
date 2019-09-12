#!/usr/bin/env python
# coding: utf-8
"""
Count how many peakers picked up trash per day.
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
        time.sleep(1.5)
    
    def write_cell(self, dest, cell, data):
        request = self.sheet.values().update(
            spreadsheetId=dest, range='Sheet1!' + cell, valueInputOption="USER_ENTERED",
            body={'values': [[data]]})
        resp = request.execute()
        time.sleep(1.5)

    def make_new_sheet(self, name):
        spreadsheet_body = {'properties': {'title': name}}
        request = self.sheet.create(body=spreadsheet_body)
        response = request.execute()
        print("New Sheet {}: {}".format(name, response['spreadsheetUrl']))
        return response


def canonicise_name(n):
    # First, strip spaces.
    n = n.strip()
    # Split into words
    words = n.split()
    # Return each word capitalized, separated by one space.
    return ' '.join([x.capitalize() for x in words])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('-c', '--count', type=int, help='How many rows to process')

    options = parser.parse_args()

    sheets = Sheets()
    raw_peaker_data = sheets.get_data(SOURCE_SPREADSHEET, "Form Responses 1!A:F")
    time_of_data = time.asctime()
    print("Got data at {}".format(time_of_data))
    if options.count is not None:
        raw_peaker_data = raw_peaker_data[1:options.count+1]
    else:
        raw_peaker_data = raw_peaker_data[1:]
    
    days = {}

    for entry in raw_peaker_data:
        if entry[3] not in days:
            days[entry[3]] = [0, 0, 0]
        if len(entry) > 5:
            if entry[5] == 'No':
                days[entry[3]][1] += 1
            elif entry[5] == 'Yes':
                days[entry[3]][0] += 1
            else:
                assert False, "Strange entry {}".format(entry[5])
        else:
            days[entry[3]][2] += 1
    print(days)