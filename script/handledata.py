import numpy as np
import pandas as pd


path = "./6.xlsx"
avg = True

data = pd.read_excel(path,header=None)
row,col = data.shape
for i in range(row):
    if "out" in data.iloc[i,0]:
        continue
    for j in range(col):
        tmp = data.iloc[i,j]
        # print(tmp)
        res = tmp.split(",")
        res = [float(a) for a in res]
        res = np.array(res)
        if avg:
            means = res.mean()
            out = str.format("%.2f"%means)
        else:
            means = (res.max()+res.min())/2.0
            offset = res.max()-res.mean()
            out = str.format("%.2f"%means)+"±"+str.format("%.2f"%offset)
        data.iloc[i,j] = out

data.to_excel("7.xlsx",header=None,index=None)


# path = r"E:\毕设\temp.xlsx"
#
# data = pd.read_excel(path,header=None)
# row = data.shape[0]
# for i in range(row):
#     x = data.iloc[i,0]
#     x = np.array([float(t) for t in x.strip().split(",")])
#     x = x.mean()
#     print(x)