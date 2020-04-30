#!/usr/bin/env python

import argparse  # create command-line tool
from getpass import getpass  # for password input without showing on console
from pymongo import MongoClient  # Mongodb API
import pandas as pd
from time import time
import os


##############################################################################
#                       Command-line features                                #
##############################################################################
prsr = argparse.ArgumentParser(description='Upload tuef strings to mongoDB')
prsr.add_argument('path', help="input file name. Relative path works fine. "
                               "Only xlsx, xls, txt and csv files are "
                               "accepted")
prsr.add_argument('--sheet', default=0,
                  help="Sheet to read from. Meaningful only for .xlsx files."
                       "Either sheet name in str or zero-indexed sheet number"
                       " in integer is accepted. Default is 0.")
prsr.add_argument('--col',
                  help="Name/number of the column in which tuef data is "
                       "stored. If name is provided it must match exact case "
                       "and special characters. Number should be specified if"
                       " and only if header is not present in the data. "
                       "Column count shall start from 0. MUST BE PROVIDED IF "
                       "INPUT FILE CONTAINS MULTIPLE COLUMNS OR COLUMN HEADER"
                  )
prsr.add_argument('--delim', default=',',
                  help="Column delimiter. Meaningful only for flat-files. "
                       "Default is ','")
prsr.add_argument('--chunk', default=100, type=int,
                  help="number of strings uploaded and processed in a chunk."
                       "Big chunksize boosts speed, but may overflow memory."
                       "Default is 100.")

prsr.add_argument('--dbstring', '-D',
                  help="connection string of mongodb instance. Must be of "
                       "format 'mongodb://<ip>:<port>/<DB name>'")
prsr.add_argument('--username', '-U', help="username for mongodb",
                  dest='uname')
prsr.add_argument('--collection', '-C', default='raw_tuef',
                  help="Collection name where data will be uploaded. Default "
                       "is 'raw_tuef'")


args = prsr.parse_args()

# -------------------------Checks for input--------------------------------- #
if args.path.split(".")[1].lower() not in ['xlsx', 'xls', 'csv', 'txt']:
    raise Exception('Invalid file extension. valid file types are: xlsx,'
                    'xls, csv and txt')
##############################################################################
#                            Connect DB                                      #
##############################################################################
if args.dbstring:
    coll = args.dbstring.split('/')[-1]  # take out the collection name from
    # the connection string
    db = MongoClient(args.dbstring)[coll]

    if args.uname:  # ask for password if username is provided
        pw = getpass("Password: ")
        db.authenticate(args.uname, pw)
else:
    db = MongoClient('mongodb://10.10.225.142:27017')['cibil']
    db.authenticate('nilabja21607', 'pass@123')


##############################################################################
#                              Read File                                     #
##############################################################################

header = None if (args.col is None) or args.col.isdigit() else 0
if args.path.split('.')[-1].lower() in ['xlsx', 'xls']:
    df = pd.read_excel(args.path, sheet_name=args.sheet, header=header)
else:
    df = pd.read_csv(args.path, sep=args.delim, header=header)
if args.col:
    if args.col.isdigit():
        tuef = df.iloc[:, int(args.col)].unique().tolist()
    else:
        tuef = df.loc[:, args.col].unique().tolist()
else:
    if df.shape[1] > 1:
        raise Exception("col must be mentioned if input table contains multiple columns")
    tuef = df.iloc[:, 0].unique().tolist()
##############################################################################
#                    Clean Tuef Data                                         #
##############################################################################


def clean(txt):
    bgn = txt.find("TUEF")
    end = txt.find('0102**')+5+1
    txt = txt[bgn:end]
    length = int(txt[-17:].replace("ES07", "")[:7])
    return {
        'string': txt,
        'valid': len(txt[bgn: end]) == length
    }
##############################################################################
#                            Push files to temp storage                      #
##############################################################################


start = time()
print('Upload of raw string commenced:\n')
step = args.chunk
for idx in range(0, len(tuef), step):
    db[args.collection].insert_many([
        clean(rec) for rec in tuef[idx:idx+step]
    ])
    perc = round(min((idx+step), len(tuef)) / len(tuef) * 100)
    elapsed = time() - start
    speed = "{:.2f} iter/sec".format(idx / elapsed) if idx > elapsed else \
        "{:.2f} sec/iter".format(elapsed/(idx+step))
    print("{}{} Speed:{}".format(u"\u2588"*perc, u"\u2501"*(100-perc), speed),
          end='\r')

print("\n\n\rUpload of raw string finished. Strings are stored in collection"
      " '{}'".format(args.collection))

# os.system('python tuef_decoder.py')