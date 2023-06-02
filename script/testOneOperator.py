import pandas as pd
import numpy as np

a = pd.read_csv("in1.csv",header=None,encoding="utf-8").to_numpy()
b = pd.read_csv("in2.csv",header=None,encoding="utf-8").to_numpy()
res_pre = pd.read_csv("out.csv",header=None,encoding="utf-8").to_numpy()

print(np.abs((a*b)-res_pre).mean()) 