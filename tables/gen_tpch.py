import pandas as pd
import sys


def DictCompress(col):
    unique_keys = col.unique()
    compress_dict = {}
    for (i, val) in enumerate(unique_keys):
        compress_dict[val] = i
    return compress_dict

#
def AddDictCompress(col, compress_dict):
    unique_keys = col.unique()
    i = len(compress_dict)
    for val in unique_keys:
        if val not in compress_dict:
            compress_dict[val] = i
            i += 1


def gen_lineitem(table_name):
    print("Cleaning up csv file")
    tbl_file = table_name + ".tbl"
    data_file = table_name + ".data"
    dfs = pd.read_csv(tbl_file, sep='|', header=None, index_col=False, chunksize=5000000, usecols=[0, 4, 5, 6, 7, 8, 9, 10])
    first = True
    returnflag_dict = {}
    linestatus_dict = {}
    max_idx = 0
    for df in dfs:
        print(df.head())
        returnflag = df[8]
        AddDictCompress(returnflag, returnflag_dict)
        linestatus = df[9]
        AddDictCompress(linestatus, linestatus_dict)
        print(returnflag_dict)
        print(linestatus_dict)
        df[8] = returnflag.map(lambda x: returnflag_dict[x])
        df[9] = linestatus.map(lambda x: linestatus_dict[x])
        if (first):
            first = False
            df.to_csv(data_file, sep='|', index=False)
        df.to_csv(data_file, sep='|', index=False, mode='a', header=False)
    print(returnflag_dict)
    print(linestatus_dict)
    print("Cleaned up csv file")

def gen_orders(table_name):
    print("Cleaning up csv file")
    tbl_file = table_name + ".tbl"
    data_file = table_name + ".data"
    df = pd.read_csv(tbl_file, sep='|', header=None, index_col=False, usecols=[0, 4, 5])
    orderpriority = df[5]
    orderpriority_dict = DictCompress(orderpriority)
    df[5] = orderpriority.map(lambda x : orderpriority_dict[x])
    df.to_csv(data_file, sep='|', index=False)
    print("Cleaned up csv file")



#### Generate CSV file ####
def gen_csv(table_name):
    if (table_name == "lineitem"):
        return gen_lineitem(table_name)
    if (table_name == "orders"):
        return gen_orders(table_name)
    print("Cleaning up csv file")
    tbl_file = table_name + ".tbl"
    data_file = table_name + ".data"
    df = pd.read_csv(tbl_file, sep='|', header=None, index_col=False)
    df.to_csv(data_file, sep='|', index=False)
    print("Cleaned up csv file")

def run(argv):
    gen_csv(argv[1])

run(sys.argv)
