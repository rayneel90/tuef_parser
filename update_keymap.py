#!/usr/bin/env python

import argparse
import pandas as pd
from pymongo import MongoClient
from getpass import getpass

prsr = argparse.ArgumentParser(
    description="Delete any existing key-mapper and upload the new key-mapper"
                "in the mongo database"
)
prsr.add_argument("path", help="path of the key-mapper file. Both relative"
                                "and absolute path works.")
prsr.add_argument('--dbstring', '-D', default="mongodb://localhost:27017/cibil",
                  help="connection string of mongodb instance. Must be of "
                       "format 'mongodb://<ip>:<port>/<DB name>'"
                       "default is 'mongo://localhost:27017/cibil'")
prsr.add_argument('--username', '-U',default=None,
                  help="username for mongodb. if not mentioned, it is assumed"
                       " that authentication is disabled for the DB")
args = prsr.parse_args()


coll = args.dbstring.split('/')[-1]
db = MongoClient(args.dbstring)[coll]
if args.username:
    pw = getpass('Enter password for user {}:'.format(args.username))
    db.authenticate(args.username, pw)

keymap = pd.read_excel(args.path, sheet_name=None, dtype=str)
for key, val in keymap.items():
    if key not in  ["reason_code", 'account_type']:
        keymap[key] = {i['Field_tag']: i['Name'] for i in
                       val.to_dict(orient='record')}
    else:
        keymap[key] = {i.pop('Field_tag'):i for i in val.to_dict(orient='record')}
db['keymap'].drop()
db['keymap'].insert_one(keymap)
print("Successfully updated keymap")
