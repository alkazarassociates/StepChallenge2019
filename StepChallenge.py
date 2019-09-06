#!/usr/bin/env python
# coding: utf-8

import collections
import os
import pickle
import sys
import time

# The Link to the spreadsheet to read data from.
SOURCE_SPREADSHEET = '1spfXPQgzg3UTTPtdKMelvbwYWVHuNFD4FWXdU5rc8vI'

# Link to source spreadsheet: https://docs.google.com/spreadsheets/d/1spfXPQgzg3UTTPtdKMelvbwYWVHuNFD4FWXdU5rc8vI/edit?ts=5d706fab#gid=2032000625
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Values provided by Google to access Sheets, from https://developers.google.com/sheets/api/quickstart/python
GoogleClientId = '773119053209-itk2e217v35j2c9covjrobv5kqs1qg1p.apps.googleusercontent.com'
ClientSecret = 'Qo7CSJjEOdNUaszEg3Nc3AX_'

# Needed tools:
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

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
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SOURCE_SPREADSHEET, range='A:F').execute()
peaker_data = result.get('values', [])
columns = ['timestamp', 'Name', 'Group', 'Day of Month', 'Footsteps', 'Litter']

print("Number of entries: {}".format(len(peaker_data)))

# print(peaker_data[0])

GroupSheets = {}
if os.path.exists('group_sheets.pickle'):
    GroupSheets = pickle.load( open('group_sheets.pickle', 'rb'))


kFlushCount = 100

def one():
    return 1


CurrentLine = collections.defaultdict(one)


def header_line():
    return [peaker_data[0]]


Buffer = collections.defaultdict(header_line)


def make_new_sheet(name):
    folder = "PeakerStepChallenge"
    spreadsheet_body = {'properties': {'title': name}}
    request = sheet.create(body=spreadsheet_body)
    response = request.execute()
    print("New Sheet {}: {}".format(name, response['spreadsheetUrl']))
    return response


for entry in peaker_data[1:]:
    group = entry[2]
    if not group in GroupSheets:
        GroupSheets[group] = make_new_sheet(group)
    dest = GroupSheets[group]
    Buffer[group].append(entry)
    if len(Buffer[group]) >= kFlushCount:
        print("Writing to {} at {}".format(group, CurrentLine[group]))
        sheet.values().update(spreadsheetId=dest['spreadsheetId'], range='Sheet1!A{}:F{}'.format(CurrentLine[group],
        CurrentLine[group] + len(Buffer[group]) - 1), valueInputOption="USER_ENTERED", body={'values': Buffer[group]}).execute()
        CurrentLine[group] += len(Buffer[group])
        Buffer[group] = list()
        time.sleep(1)

print("Final Flush:")
for group in Buffer:
    if Buffer[group]:
        print("Writing to {} at {} up to {}".format(group, CurrentLine[group], CurrentLine[group] + len(Buffer[group])))
        ret = sheet.values().update(spreadsheetId=GroupSheets[group]['spreadsheetId'], range='Sheet1!A{}:F{}'.format(CurrentLine[group],
        CurrentLine[group] + len(Buffer[group]) - 1), valueInputOption="USER_ENTERED", body={'values': Buffer[group]}).execute()
        CurrentLine[group] += len(Buffer[group])
        Buffer[group] = list()
        time.sleep(1)

pickle.dump(GroupSheets, open('group_sheets.pickle', 'wb'))
