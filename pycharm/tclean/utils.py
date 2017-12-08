#!/usr/bin/env python

"""This is the example module.

This module does stuff.
"""
import collections as cols
import re, unicodedata

import numpy as np

__author__ = "Wai Lam Jonathan Lee"
__email__ = "walee@uc.cl"


def update(d0, d1):
    d2 = dict(d0)
    d2.update(d1)
    return d2


def safeget(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
            if dct == None:
                return None
        except KeyError:
            return None
    return dct


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, cols.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def strip_accents(s):
    if s:
        return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')
    else:
        return s


def clean_string(s):
    if not s or s == 'None':
        return np.nan
    aux = strip_accents(s)
    aux = re.sub('[^0-9a-zA-Z/]+', ' ', str(aux))
    return aux.strip()