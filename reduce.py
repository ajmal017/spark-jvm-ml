from __future__ import print_function


from io import StringIO
import os
import re
import pandas as pd

def GetConfigsDictionary(memorySize):
    # return true if there is another option, the args to pass and a name for a file
    heapSizesDriver = [str(i+1) for i in range(1, int(memorySize))] #Can't have 0g of ram...
    heapSizesExec = [str(i+1) for i in range(1, int(memorySize))]

    d = dict()

    algorithms = ["+UseParallelGC", "+UseG1GC"]
    instanceCount = 0

    d['headers'] = "driver-memory,executor-memory,spark.executor.extraJavaOptions=-XX:"
    for hd in heapSizesDriver: # 4
        for he in heapSizesExec: # 4
            #for th in threshold:
            for alg in algorithms: # 2
                args = "--driver-memory " + hd + " --executor-memory " + he +" --conf spark.executor.extraJavaOptions=-XX:" + alg
                c = str(instanceCount)
                instanceCount += 1

                d[c] = f'{hd},{he},{alg}'
    
    return d


def ExtractOutputAsDict(path):

    fileList = os.listdir(path)
    d = dict()
    d['headers'] = "runtime"
    for f in fileList:
        q = f[0:4]
        c = f[11:-4]
        with open(path + f, 'r') as myfile:
            data = myfile.read()
            time  = re.search("(\\d+\\.\\d+)(?=\\Wseconds)", data).group(1)
            d[(q, c)] = time
            pass
    return d

def GetCsvAsString(configsDictionary, outputDataDictionary):
    # outputDataDictionary = dict()
    s = ""
    s+= f"query_number,{configsDictionary['headers']},{outputDataDictionary['headers']}\n"
    for o in outputDataDictionary.keys():
        if o == 'headers':
            continue
        q, c = o
        s+=f"{q},{configsDictionary[c]},{outputDataDictionary[o]}\n"
        pass
    return s


    

if __name__ == "__main__":
    #MUST SET PATH TO OUTPUT DATA
    inPath = '/home/goodlad/bench/spark-tpc-ds-performance-test/output/'
    outPath = ''

    configs =  GetConfigsDictionary(4)
    outputData = ExtractOutputAsDict(inPath)
    csvAsString = GetCsvAsString(configs, outputData)

    df = pd.read_csv(StringIO(csvAsString), sep=',')
    df.to_csv(outPath + 'out.csv')
    df_driver_memory = df['driver-memory']
    df_executor_memory = df['executor-memory']


    df_queries_onehot = pd.get_dummies(df['query_number'], prefix='query_number')
    df_gc_onehot = pd.get_dummies(df['spark.executor.extraJavaOptions=-XX:'], prefix='spark.executor.extraJavaOptions=-XX:')

    df_final_onehot = pd.concat([df_queries_onehot, df_driver_memory, df_executor_memory, df_gc_onehot], 1)

    df_final_onehot.to_csv(outPath + 'out_onehot.csv')
    print(df_final_onehot)
    pass