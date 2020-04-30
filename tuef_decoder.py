#!/usr/bin/env python

import argparse  # create command-line tool
from getpass import getpass  # for password input without showing on console
from pymongo import MongoClient, errors # Mongodb API
import pandas as pd
from datetime import datetime
import re
from time import time
from datetime import timedelta
##############################################################################
#                       Command-line features                                #
##############################################################################
prsr = argparse.ArgumentParser(
    description='Process raw tuef strings read from mongodb'
)
prsr.add_argument('--dbstring', '-D',
                  help="connection string of mongodb instance. Must be of "
                       "format 'mongo://<ip>:<port>/<collection>'")
prsr.add_argument('--username', '-U', help="username for mongodb",
                  dest='uname')
prsr.add_argument('--in-coll', '-I', default='raw_tuef',
                  help="Collection name where the raw strings are stored. "
                       "Default is 'raw_tuef'")
prsr.add_argument('--out-coll', '-O', default='tuef',
                  help="Collection name where final processed report string "
                       "will be stored. default is 'tuef")
prsr.add_argument('--chunk', default=10, type=int,
                  help="number of strings processed in a chunk."
                       "Big chunksize marginally boosts speed."
                       "Default is 10.")
args = prsr.parse_args()

print("\n Processing Commencing...\n")
# -------------------------Checks for input--------------------------------- #
##############################################################################
#                            Connect DB                                      #
##############################################################################
if args.dbstring:
    coll = args.dbstring.split('/')[-1]
    db = MongoClient(args.dbstring)[coll]
    if args.uname:
        pw = getpass("Password: ")
        db.authenticate(args.uname, pw)
else:
    db = MongoClient('mongodb://10.10.225.142:27017')['cibil']
    db.authenticate('nilabja21607', 'pass@123')

##############################################################################
#                           Custom Functions                                 #
##############################################################################


def multi_chunk_breaker(string: str, splitter: str, shift: int = 2) -> list:
    """ Compute ... and return ...
    :return:
    :param string:
    :param splitter:
    :param shift:
    :return:
    """
    chunk = string.split(splitter)
    chunk.pop(0)
    return [str_breaker(indx[shift:]) for indx in chunk]


def str_breaker(string):
    """
    :param string:
    :return:
    """
    ret = {}
    while string:
        key = string[:2]
        string = string[2:]
        length = int(string[:2])
        string = string[2:]
        val = string[:length]
        string = string[length:]
        ret[key] = val
    return ret


# the tuef contains several segments which have definite start values


keymap = db['keymap'].find_one()

pattern = re.compile(
    "(PN03N01|ID03I01|PT03T01|EC03C01|EM03E01|SC10CIBILTUSCR|PA03A01|"
    "TL04T001|IQ04I001|DR03D01)"
)  # Create the pattern to break the tuef string into segments

total = db[args.in_coll].count_documents({'valid':True})

start = time()
idx = 0
succ_cnt = 0
fail_cnt = 0
while 1:
    tuef = db[args.in_coll].find_one_and_delete({'valid': True})
    if not tuef:
        break
    tuef_bac = tuef
    try:
        tuef = tuef['string'][:-17]
        chunkdict = {}
        breaks = [m.start() for m in pattern.finditer(tuef)]
        breaks.insert(0, 0)
        breaks.append(len(tuef))

        for i in range(1, len(breaks)):
            temp = tuef[breaks[i - 1]:breaks[i]]
            chunkdict[temp[:2]] = temp

        header = {}
        header["seg_tag"], tuef = tuef[:4], tuef[4:]
        header["version"], tuef = tuef[:2], tuef[2:]
        header["mem_ref_no"], tuef = tuef[:25].strip(), tuef[25:]
        header["blank"], tuef = tuef[:6], tuef[6:]
        header["enq_mem_user_id"], tuef = tuef[:30].strip(), tuef[30:]
        header["sub_ret_cd"], tuef = tuef[:1], tuef[1:]
        header["enq_cntrl_no"], tuef = tuef[:12], tuef[12:]
        header["datetime_processed"] = datetime.strptime(
            tuef[:14], '%d%m%Y%H%M%S')
        ######################################################################
        #                                 PN decoding                        #
        ######################################################################

        name_chunk = chunkdict.get('PN')
        if name_chunk:
            name = str_breaker(name_chunk[7:])
            name = {keymap['PN'][key]: val for key, val in name.items()}
            if 'gender' in name:
                name['gender'] = keymap['gender'][name['gender']]
            if 'dob' in name:
                name['dob'] = datetime.strptime(name['dob'], '%d%m%Y')
            name = {key: val for key, val in name.items() if val is not None}
            header.update(name)
        ######################################################################
        #                                 ID decoding                        #
        ######################################################################

        id_chunk = chunkdict.get('ID')
        if id_chunk:
            ids = pd.DataFrame(multi_chunk_breaker(id_chunk, "ID03I"))
            ids.columns = [keymap['ID'][key] for key in ids.columns]
            if "id_type" in ids:
                ids["id_type"] = [keymap["id_type"][i] for i in
                                  ids["id_type"]]
            if "issue_date" in ids:
                ids["issue_date"] = pd.to_datetime(ids["issue_date"],
                                                   format='%d%m%Y')
            if "expiration_date" in ids:
                ids["expiration_date"] = pd.to_datetime(
                    ids["expiration_date"], format='%d%m%Y')
            header['ids'] = ids.T.apply(lambda x: x.dropna().to_dict()).\
                tolist()
        ######################################################################
        #                            PT decoding                             #
        ######################################################################

        phone_chunk = chunkdict.get('PT')
        if phone_chunk:
            tele = pd.DataFrame(multi_chunk_breaker(phone_chunk, "PT03T"))
            tele.columns = [keymap['PT'][key] for key in tele.columns]
            if "telephone_type" in tele:
                tele["telephone_type"] = [keymap["telephone_type"][i] for i in
                                          tele["telephone_type"]]
            header['phones'] = tele.T.apply(
                lambda x: x.dropna().to_dict()).tolist()
        ######################################################################
        #                                 EC decoding                        #
        ######################################################################

        email_chunk = chunkdict.get('EC')
        if email_chunk:
            email = pd.DataFrame(multi_chunk_breaker(email_chunk, "EC03C"))
            email.columns = ['email']
            header['emails'] = \
                email.email.str.lower().dropna().unique().tolist()

        ######################################################################
        #                            EM decoding                             #
        ######################################################################

        employ_chunk = chunkdict.get('EM')
        if employ_chunk:
            employ = str_breaker(employ_chunk[7:])
            employ = {keymap['EM'][key]: val for key, val in employ.items()}
            employ['account_type'] = keymap['account_type'][
                employ['account_type']]
            employ['date_reported_and_certified'] = datetime.strptime(
                employ['date_reported_and_certified'], '%d%m%Y')
            employ['occupation_code'] = keymap['occupation_code'].get(
                employ.get('occupation_code'))
            employ['net_gross_income_indicator'] = \
                keymap['net_gross_income_indicator'].get(employ.get(
                    'net_gross_income_indicator'))
            employ['monthly_annual_income_indicator'] = \
                keymap['monthly_annual_income_indicator'].get(
                    employ.get('monthly_annual_income_indicator'))
            employ = {key: val for key, val in employ.items()
                      if val is not None}
            header.update(employ)
        ######################################################################
        #                       SC decoding                                  #
        ######################################################################

        score_chunk = chunkdict.get("SC")
        if score_chunk:
            score = str_breaker(score_chunk[14:])
            score = {keymap['SC'][key]: val for key, val in score.items()}
            score['score_date'] = datetime.strptime(score['score_date'],
                                                    '%d%m%Y')
            score = {key: val for key, val in score.items()
                     if val is not None}
            header.update(score)
        ######################################################################
        #                                 PA decoding                        #
        ######################################################################

        addr_chunk = chunkdict.get("PA")
        if addr_chunk:
            addr = pd.DataFrame(multi_chunk_breaker(addr_chunk, "PA03A"))
            addr.columns = [keymap['PA'][key] for key in addr.columns]
            if 'date_reported' in addr:
                addr['date_reported'] = pd.to_datetime(addr['date_reported'],
                                                       format='%d%m%Y')
            if 'state_code' in addr:
                addr['state_code'] = [keymap['state_code'].get(i) for i in
                                      addr['state_code']]
            if 'address_category' in addr:
                addr['address_category'] = [keymap['address_category'].get(i)
                                        for i in addr['address_category']]
            header['addresses'] = addr.T.apply(
                lambda x: x.dropna().to_dict()).tolist()

        ######################################################################
        #                       TL decoding                                  #
        ######################################################################

        acc_chunk = chunkdict.get('TL')
        if acc_chunk:
            acc = multi_chunk_breaker(acc_chunk, "TL04T", 3)
            summary = pd.DataFrame([i for i in acc if
                                    i['02'] == "ACCTREVIEW_SUMM"])
            acc = pd.DataFrame([i for i in acc if i['02']
                                != "ACCTREVIEW_SUMM"])
            acc.columns = [keymap['TL'][key] for key in acc.columns]
            summary.columns = [keymap['summary'][key] for
                               key in summary.columns]
            acc['account_name'] = [keymap['account_type'][i]['Name'] for i in
                                   acc['account_type']]
            acc['account_type'] = [keymap['account_type'][i]['loan_type'] for i in
                                   acc['account_type']]
            acc['ownership_indicator'] = [keymap['ownership_indicator'][i] for
                                          i in acc['ownership_indicator']]
            datecols = list({'date_opened_disbursed', 'date_of_last_payment',
                             'date_closed', 'date_reported_certified',
                             'payment_history_start_date',
                             'payment_history_end_date'}.intersection(
                acc.columns))
            acc[datecols] = acc[datecols].apply(pd.to_datetime,
                                                format="%d%m%Y")
            if 'suit_filed_wilful_default' in acc:
                acc['suit_filed_wilful_default'] = \
                    [keymap['suit_filed_wilful_default'].get(i) for i in
                     acc['suit_filed_wilful_default']]
            header['accounts'] = acc.T.apply(
                lambda x: x.dropna().to_dict()).tolist()
        ######################################################################
        #                                 IQ decoding                        #
        ######################################################################
        enquiry_chunk = chunkdict.get('IQ')
        if enquiry_chunk:
            enq = pd.DataFrame(multi_chunk_breaker(enquiry_chunk, "IQ04I", 3))
            enq.columns = [keymap['IQ'][key] for key in enq.columns]
            enq['date_of_enquiry'] = pd.to_datetime(enq['date_of_enquiry'],
                                                    format='%d%m%Y')
            enq['enquiry_purpose'] = [keymap['account_type'][i] for i in
                                      enq['enquiry_purpose']]
            enq['enquiry_purpose_acct_nm'] = [keymap['account_type'][i]['Name'] for i in
                                   enq['enquiry_purpose']]
            enq['enquiry_purpose_acct_typ'] = [keymap['account_type'][i]['loan_type'] for i in
                                   enq['enquiry_purpose']]
            header['enquiries'] = enq.T.apply(
                lambda x: x.dropna().to_dict()).tolist()
    except Exception as e:
        tuef_bac['error'] = e.args
        db['failed'].insert_one(tuef_bac)
        fail_cnt = fail_cnt + 1
    else:
        db[args.out_coll].insert_one(header)
        succ_cnt = succ_cnt + 1
    idx += 1
    if not idx % args.chunk:
        perc = round(100 * idx / total)
        elapsed = time() - start
        speed = "{:.2f} iter/sec".format(idx / elapsed) if idx > elapsed else \
            "{:.2f} sec/iter".format(elapsed / (idx + args.chunk))
        eta = str(timedelta(seconds = round(elapsed * (total-idx)/idx)))
        print("{}{} Speed:{} | ETA: {}".format(
            u"\u2588" * perc, u"\u2501" * (100 - perc), speed, eta), end='\r')
print(
    """
        Processing Completed.
        # Successful Conversions: {}
        # Failed cases: {}
        Collection containing Successful entries: {}
        Collection failed cases: {}
    """ .format( succ_cnt, fail_cnt, args.out_coll, 'failure'))
