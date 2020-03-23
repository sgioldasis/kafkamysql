import datetime
import pandas as pd
import numpy as np

t = pd.to_datetime('2020-03-22T00:21:32.1878918Z')
print(t)

n = t.value
print(n)

# #Convert datetime to epoch.
# value = (datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ") - datetime.datetime(1970,1,1)).total_seconds()
# print( value )
# #Convert epoch to datetime. 
# print( datetime.datetime.fromtimestamp(value) )