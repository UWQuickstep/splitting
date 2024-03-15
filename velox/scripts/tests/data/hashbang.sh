#!/bin/bash
import argparse
import fnmatch
import os
import regex
import sys

class attrdict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__

def parse_args():
    parser = argparse.ArgumentParser(description='Update license headers')
    parser.add_argument('--header', default='license.header', help='header file')
    parser.add_argument('--extra', default=30,
        help='extra characters past beginning of file to look for header')
    parser.add_argument('--editdist', default=7, help='max edit distance between headers')
    parser.add_argument('--remove', default=False, action="store_true",
        help='remove the header')
    parser.add_argument('--cslash', default=False, action="store_true",
        help='use C slash "//" style comments')
    parser.add_argument('-v', default=False, action="store_true", dest="verbose",
        help='verbose output')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-k', default=False, action="store_true", dest="check",
        help='check headers')
    group.add_argument('-i', default=False, action="store_true", dest="inplace",
        help='edit file inplace')

    parser.add_argument('files', metavar='FILES', nargs='+',
        help='files to process')

    return parser.parse_args()
