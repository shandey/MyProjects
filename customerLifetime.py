#!/usr/bin/python

from itertools import groupby
from operator import itemgetter
from datetime import datetime
from datetime import timedelta
import uuid
import sys

## number of days before a customer is considersed inactive
break_day_val = int(sys.argv[1])   

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    data = read_mapper_output(sys.stdin, separator=separator)
    for key, group in groupby(data, itemgetter(0)):
        visits = sorted([int(x[1]) for x in list(group)])
        prev_uuid = None
        prev_date = None
        results = []
        for v in visits:
            new_uuid = str(uuid.uuid4())
            if not prev_uuid: ## 1st record in group
                results.append((v,new_uuid))
                prev_uuid = new_uuid
            elif datetime.strptime(str(prev_date),"%Y%m%d") + timedelta(days=break_day_val) < datetime.strptime(str(v),"%Y%m%d"):  ## greater than threshold
                results.append((v,new_uuid))
                prev_uuid = new_uuid
            else:  ## new session
                results.append((v,prev_uuid))

            prev_date = v   


        for lifetime_hash, group in groupby(results, itemgetter(1)):            
            lifetime_visit_dates =[y[0] for y in list(group)]
            print str((datetime.strptime(str(lifetime_visit_dates[-1]),"%Y%m%d") -  datetime.strptime(str(lifetime_visit_dates[0]),"%Y%m%d")).days +1) + "\t" + '1'

            


if __name__ == "__main__":
    main()
