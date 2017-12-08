import functools as fct
import json, os, math, gc
import numpy as np
import pandas as pd
from tclean.utils import safeget, flatten, clean_string
import multiprocessing
from multiprocessing import Queue


def is_valid(tender):
    if 'Listado' in tender and len(tender['Listado']) > 0:
        return True
    return False


def has_zero_tprod(tender):
    if tender['Listado'][0]['Items']['Cantidad'] == 0:
        return True
    return False


def get_valid_tenders(tid_list, tdict):
    # check if some of the tenders need to removed
    rmlist = list()
    for tid in tid_list:
        tender = tdict[tid]
        if is_valid(tender) and not has_zero_tprod(tender):
            # update tender
            tender = tender['Listado'][0]
            tender_list.append((tid, tender))
        else:
            rmlist.append(tid)
    tid_list = [tid for tid in tid_list if tid not in rmlist]
    tdict = {item[0]:item[1] for item in tender_list}
    return tid_list, tdict


def flatten_info(tdict):
    # create dict for tenders
    tdict_flat = dict()
    for tid, tender in tdict.items():
        if type(tender) == str:
            # print(tid, tender)
            continue
        tdict_flat[tid] = flatten(tender, sep='')

    # create dict for tender products
    tprod_dict = dict()
    for tid, tender in tdict_flat.items():
        tprod_dict[tid] = tender['ItemsListado']
        del tender['ItemsListado']

    return tdict_flat, tprod_dict


def create_tender_df(tdict_flat, colname_list, tid_list):
    tdict_cols = dict()
    for colname in colname_list:
        value_list = []
        for tid in tid_list:
            tender = tdict_flat[tid]
            value = safeget(tender, colname)
            value_list.append(value)
        tdict_cols[colname] = value_list

    tender_df = pd.DataFrame.from_dict(tdict_cols, orient='columns')
    return tender_df


def flatten_tprod_list(tid_list, tprod_dict, tender_df):
    tprod_flat_list = []
    for tid in tid_list:
        tprod_list = tprod_dict[tid]
        tender_prod = 0
        for tprod in tprod_list:
            # flatten the dict
            tid_list = flatten(tprod, sep='')
            # add the tid as feature
            tid_list['CodigoExterno'] = tid
            tprod_flat_list.append(tid_list)
            tender_prod += 1
        try:
            test_tender = tender_df[(tender_df['CodigoExterno'] == tid)]
            assert tender_prod == test_tender['ItemsCantidad'].values[
                0], test_tender
        except Exception as e:
            print('Exception: {}, {}'.format(e, tid))
    return tprod_flat_list


def create_tprod_df(tprod_flat_list):
    # ### Get all the possible keys and convert to Dataframe
    keys = fct.reduce(lambda k, d: list(set(k).union(set(d.keys()))),
                      tprod_flat_list, [])
    tprod_cols = dict()
    colname_list = sorted(list(keys))
    for colname in colname_list:
        value_list = []
        for tprod in tprod_flat_list:
            value = safeget(tprod, colname)
            value_list.append(value)
        tprod_cols[colname] = value_list

    return pd.DataFrame.from_dict(tprod_cols, orient='columns')


def convert_to_dfs(tdict):
    # get the list of tids
    tid_list = list(tdict.keys())
    tid_list = sorted(tid_list)

    # get the valid tenders only
    tid_list, tdict = get_valid_tenders(tid_list, tdict)

    # create flattened dicts
    tdict_flat, tprod_dict = flatten_info(tdict)

    # create tender df
    tid0 = tid_list[0]
    colname_list = sorted(list(tdict_flat[tid0].keys()))
    tender_df = create_tender_df(tdict_flat, colname_list, tid_list)

    # process tender products separately
    tprod_flat_list = flatten_tprod_list(tid_list, tprod_dict, tender_df)
    tprod_df = create_tprod_df(tprod_flat_list)

    # ### Remove the attribute Adjudicacion
    if 'Adjudicacion' in tprod_df.columns:
        tprod_df.drop('Adjudicacion', inplace=True, axis=1)

    # skip columns containing rut and code
    skip_criteria = lambda colname: 'Rut' in colname or 'Codigo' in colname or 'Fecha' in colname or 'Url' in colname
    for col in tender_df.columns:
        if not np.issubdtype(tender_df[col].dtype, np.number) and not skip_criteria(col):
            # normalize accented strings and remove non-alphanumerical chars
            tender_df[col] = list(map(lambda s: clean_string(s), tender_df[col].values))
            
    for col in tprod_df.columns:
        if not np.issubdtype(tprod_df[col].dtype, np.number) and not skip_criteria(col):
            # normalize accented strings and remove non-alphanumerical chars
            tprod_df[col] = list(map(lambda s: clean_string(s), tprod_df[col].values))
            
    tender_df.fillna(value=np.nan, inplace=True)
    tprod_df.fillna(value=np.nan, inplace=True)


    # ### Dealing with duplicates
    tprod_duplicated = tprod_df[tprod_df.duplicated()]
    # update tprod df
    tprod_df = tprod_df.drop_duplicates()
    # update tender_df
    grouped_by_tid = tprod_duplicated.groupby('CodigoExterno').size().reset_index(drop=False)
    grouped_by_tid.columns = ['CodigoExterno', 'ItemsCantidadNew']

    # join
    tender_df = pd.merge(left=tender_df, right=grouped_by_tid, on='CodigoExterno', how='left')
    tender_df['tender_product_duplicated'] = tender_df['ItemsCantidad'] - tender_df['ItemsCantidadNew']

    tid_duplicated = set(tprod_duplicated['CodigoExterno'].unique())
    tender_duplicated = tender_df[(tender_df['tender_product_duplicated'] > 0)]
    if len(tid_duplicated) > 0:
        # print(tid_duplicated)
        # print(set(tender_duplicated['CodigoExterno'].unique()))
        assert tid_duplicated == set(tender_duplicated['CodigoExterno'].unique())

    tender_df.drop('ItemsCantidadNew', inplace=True, axis='columns')
    return tender_df, tprod_df


def mp_clean_export(tlist, tdir, nprocs):
    out_q = Queue()
    chunksz = int(math.ceil(len(tlist)) / float(nprocs))
    procs = []

    def worker(i, tsublist, tdir, out_q):
        outdict = {}
        for json_file in tsublist:
            if '.json' not in json_file:
                continue
            tender_id = json_file.replace('.json', '')
            tender_fpath = os.path.join(tdir, json_file)
            with open(tender_fpath, 'r') as f:
                try:
                    tender_json = json.load(f)
                    tender_dict[tender_id] = tender_json
                except Exception as e:
                    print('{} \ntid: \n{}'.format(e, tender_id))
        tender_df, tprod_df = convert_to_dfs(tender_dict)
        outdict[i] = (tender_df, tprod_df)
        out_q.put(outdict)

    for i in range(nprocs):
        print('Process: {}'.format(i))
        start = chunksz * i
        end = chunksz * (i + 1)
        # compute the chunk
        if i < nprocs - 1:
            tsublist = tender_list[start:end]
        else:
            tsublist = tender_list[start:]
        p = multiprocessing.Process(
            target=worker,
            args=(i, tsublist, tdir, out_q)
        )
        procs.append(p)
        p.start()

    # collect all results into a single result dict
    resultdict = dict()
    for i in range(nprocs):
        outdict = out_q.get()
        resultdict.update(outdict)
    # each dict consists of a tender df and a tprod df
    tender_df = pd.DataFrame()
    tprod_df = pd.DataFrame()
    for item in resultdict.values():
        tender_df_i = item[0]
        tprod_df_i = item[1]
        if tender_df.empty and tprod_df.empty:
            tender_df = tender_df_i
            tprod_df = tprod_df_i
        else:
            tender_df = pd.concat([tender_df, tender_df_i], axis=0)
            tprod_df = pd.concat([tprod_df, tprod_df_i], axis=0)

    # wait for all worker processes to finish
    for p in procs:
        p.join()

    return tender_df, tprod_df


if __name__ == '__main__':
    base_dir = os.path.join('..', '..', 'data')
    tender_dir = os.path.join('..', '..', 'data', '2016')
    processed_dir = os.path.join(base_dir, 'processed2016')

    if not os.path.isdir(processed_dir):
        os.makedirs(processed_dir)

    batch = 30000
    tender_list = os.listdir(tender_dir)
    num_of_batches = int(math.ceil(len(tender_list) / batch))
    print('Number of batches: {}'.format(num_of_batches))

    for i in range(num_of_batches):
        print('Cleaning and exporting batch: {}'.format(i))
        tender_dict = dict()
        start = i * batch
        if i < num_of_batches - 1:
            end = start + batch
            file_list = tender_list[start:end]
        else:
            file_list = tender_list[start:]

        # nprocs = multiprocessing.cpu_count() - 1
        # tender_df, tprod_df = mp_clean_export(file_list, tender_dir, nprocs)

        for json_file in file_list:
            if '.json' not in json_file:
                continue
            tender_id = json_file.replace('.json', '')
            tender_fpath = os.path.join(tender_dir, json_file)
            with open(tender_fpath, 'r') as f:
                try:
                    tender_json = json.load(f)
                    tender_dict[tender_id] = tender_json
                except Exception as e:
                    print('{} \ntid: \n{}'.format(e, tender_id))
        tender_df, tprod_df = convert_to_dfs(tender_dict)

        tender_fname = 'tender{}.csv'.format(i)
        tender_fpath = os.path.join(processed_dir, tender_fname)
        tprod_fname = 'tenderProduct{}.csv'.format(i)
        tprod_fpath = os.path.join(processed_dir, tprod_fname)

        # export them
        tender_df.to_csv(tender_fpath, index=False, sep=',')
        tprod_df.to_csv(tprod_fpath, index=False, sep=',')
        gc.collect()