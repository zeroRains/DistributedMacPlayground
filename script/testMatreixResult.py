import pandas as pd
import numpy as np

a = pd.read_csv("in1.csv",header=None,encoding="utf-8").to_numpy()
b = pd.read_csv("in2.csv",header=None,encoding="utf-8").to_numpy()
# c = pd.read_csv("in3.csv",header=None,encoding="utf-8").to_numpy()
res_pre = pd.read_csv("out.csv",header=None,encoding="utf-8").to_numpy()
print(a.shape)
print(b.shape)
# print(c.shape)
print(res_pre.shape)

# a = a.transpose()
res = a.dot(b)
# res = res.dot(c)

print(np.abs(res-res_pre).mean())

