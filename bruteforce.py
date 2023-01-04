#
# This script is used to get the actual results of the requested tasks
# using brure-force method. It is slow but it is working...
# Don't use it with a lot of data or you PC will explode...
#

import pandas as pd, argparse

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--data", help = "Data to plot", default=None)
parser.add_argument("-t", "--top", help = "Number of top points in terms of dominations", default=None)

args = parser.parse_args()

if args.data == None or args.top == None:
    parser.print_help()
    exit(0)

top = int(str(args.top))
data = pd.read_csv(args.data,dtype=float,header=None)

dim = len(data.loc[0,:])
lines = len(data)

data["dom"] = 0 #Dominations
data["skl"] = True

def dominates(a,b):
    dom = True
    for k in range(dim):
        if a[k] > b[k]:
            dom = False
            break
    return dom

for i in range(lines):
    for j in range(lines):
        if i == j: continue
        data.loc[i, "dom"] += 1 if dominates(data.loc[i,:],data.loc[j,:]) else 0
        if data.loc[i, "skl"]: #if marked as a skyline points, check if it is true
            if (dominates(data.loc[j,:],data.loc[i,:])):
                data.loc[i, "skl"] = False #Not a skyline point, it is dominated

data.loc[data["skl"] == True].to_csv("skyline_points.csv")
data.sort_values(by="dom",ascending=False).head(top).to_csv("topk_points.csv")
data.loc[data["skl"] == True].sort_values(by="dom",ascending=False).head(top).to_csv("topk_skyline_points.csv")
