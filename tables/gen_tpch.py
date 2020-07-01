import pandas as pd
import sys


def DictCompress(col):
    unique_keys = col.unique()
    sorted(unique_keys)
    compress_dict = {}
    for (i, val) in enumerate(unique_keys):
        compress_dict[val] = i
    return compress_dict


def gen_lineitem(table_name):
    print("Cleaning up csv file")
    tbl_file = table_name + ".tbl"
    data_file = table_name + ".data"
    df = pd.read_csv(tbl_file, sep='|', header=None, index_col=False)
    returnflag = df[8]
    returnflag_dict = DictCompress(returnflag)
    linestatus = df[9]
    linestatus_dict = DictCompress(linestatus)
    df[8] = returnflag.map(lambda x: returnflag_dict[x])
    df[9] = linestatus.map(lambda x: linestatus_dict[x])
    df.to_csv(data_file, sep='|', index=False)
    print("Cleaned up csv file")


#### Generate CSV file ####
def gen_csv(table_name):
    if (table_name == "lineitem"):
        return gen_lineitem(table_name)
    print("Cleaning up csv file")
    tbl_file = table_name + ".tbl"
    data_file = table_name + ".data"
    df = pd.read_csv(tbl_file, sep='|', header=None, index_col=False)
    df.to_csv(data_file, sep='|', index=False)
    print("Cleaned up csv file")

def run(argv):
    gen_csv(argv[1])

run(sys.argv)
