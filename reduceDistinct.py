#!/usr/bin/python

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file):
    for line in file:
        yield line.rstrip()

def main():
    data = read_mapper_output(sys.stdin)

    for schema in groupby(data):
        try:
            print schema[0]
        except:
            pass

if __name__ == "__main__":
    main()
