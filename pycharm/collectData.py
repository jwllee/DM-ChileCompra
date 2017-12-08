#!/usr/bin/env python
import requests
import pandas as pd
import numpy as np
import os, sys, json
import time
import calendar


__author__ = "Wai Lam Jonathan Lee"
__email___ = "walee@uc.cl"


def make_query(query, max_tries=3, sleep=2):
    num_of_tries = 0
    req = None
    while num_of_tries < max_tries and not req:
        try:
            req = requests.get(query)
            if 'Cantidad' not in req.json():
                # some error, reload
                print('({}) {}'.format(tender_id, req.json()))
                req = None
                time.sleep(sleep)
                num_of_tries += 1
        except Exception as e:
            print(e)
    return req


if __name__ == '__main__':
    '''
    tender_ids_df = pd.read_csv('../../data/processed/toDownload30Aug.csv')
    tender_ids = tender_ids_df['tender_id']
    num_of_tenders = len(tender_ids)
    export_dir = '../../data/raw/toDownload'
    if not os.path.isdir(export_dir):
       os.makedirs(export_dir)

    # existing tenders
    existing_tenders = os.listdir(export_dir)
    existing_tenders = list(map(lambda t: t.replace('.json', ''), \
        existing_tenders))
    '''

    export_dir = './data'
    if not os.path.isdir(export_dir):
        os.makedirs(export_dir)

    # import tids
    data = dict()
    tid_dir = os.path.join('.', 'tid')
    for tid_path in os.listdir(tid_dir):
        date = tid_path.replace('.csv', '')
        date = date.replace('tid', '')
        tid_path = os.path.join(tid_dir, tid_path)
        tid_df = pd.read_csv(tid_path)
        data[date] = tid_df['Textbox36'].values
#        print('{}: {}'.format(date, tid_df['Textbox36'].values))

    base = 'http://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?'
    ticket = '0451A89D-4612-4A08-9B94-D6FB9AECBD35'

    keys = ['201612', '201701', '201702', '201703']
    for date in keys:
        tids = data[date]
        print('Fetching tender info for {}'.format(date))
        # make directory
        date_dir = os.path.join(export_dir, date)
        if not os.path.isdir(date_dir):
            os.makedirs(date_dir)
        cnt = 0
        for tid in tids:
#            print('Fetching tid: {}'.format(tid))
            # check if directory already has this tid
            fetched = os.listdir(date_dir)
            fetched = list(map(lambda f: f.replace('.json', ''), fetched))
            if tid not in fetched:
                tid_query = 'codigo={}'.format(tid)
                ticket_query = 'ticket={}'.format(ticket)
                query = base + tid_query + '&' + ticket_query
                req = make_query(query)
                if req:
                    # export it 
                    export_path = os.path.join(date_dir, tid + '.json')
                    with open(export_path, 'w') as outfile:
                        json.dump(req.json(), outfile)
                time.sleep(2)
            else:
                print('tid {} already fetched'.format(tid))
            cnt += 1
            print('Finished {}/{} tids'.format(cnt, len(tids)))

#    org_code = 7248
#    yr_mnth_list = [(2016, 12), (2017, 1), (2017, 2)]
#    for year, month in yr_mnth_list:
#        days = calendar.monthrange(year, month)
#        for i in range(days):
#            day = i + 1
#            date = '{day}{month}{year}'.format(day=day, month=month, year=year)
#            date_query = 'fecha={date}'.format(date=date)
#            org_query = 'CodigoOrganismo={}'.format(org_code)
#            ticket_query = 'ticket={}'.format(ticket)
#            query = base + date_query + '&' + org_query + '&' + ticket_query
#            req = make_query(query)
#            if req:
#                export_path = export_dir + os.sep + date + '.json'
#                with open(export_path, 'w') as outfile:
#                    json.dump(req.json(), outfile)
#            # wait for 2 seconds before next request
#            time.sleep(2)
#
#
#    for ind in range(len(tender_ids)):
#        tender_id = tender_ids[ind]
#        if tender_id in existing_tenders:
#            print('Skipping existing tender: {}'.format(tender_id))
#            continue
#            url = 'http://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?codigo={tender_id}&ticket=F8537A18-6766-4DEF-9E59-426B4FEE2844'.format(tender_id=tender_id.strip())
#            r = None
#            num_of_tries = 0
#        while r == None:
#            try:
#                r = requests.get(url)
#                if 'Cantidad' not in r.json():
#                    # some error, reload
#                    print('({}) {}'.format(tender_id, r.json()))
#                    r = None
#                    time.sleep(4)
#                    num_of_tries += 1
#            except Exception as e:
#                print(e)
#            if num_of_tries > 3:
#                break
#        if num_of_tries > 3:
#            # skip this tender download
#            print('Skipping {}'.format(tender_id))
#            continue
#        print('Finished fetching {}/{}'.format(ind, num_of_tenders))
#        export_path = export_dir + os.sep + tender_id + '.json'
#        with open(export_path, 'w') as outfile:
#            json.dump(r.json(), outfile)
#        # wait for 5 seconds before next request
#        if ind != num_of_tenders:
#            time.sleep(4)



