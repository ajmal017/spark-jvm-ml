from __future__ import print_function

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--ram', dest='ram', default=4, help='number of gigabytes you have for RAM/want to use')


def GetNextConfig(memorySize):
    # return true if there is another option, the args to pass and a name for a file
    heapSizesDriver = [str(i+1) + "g" for i in range(1, int(memorySize))] #Can't have 0g of ram...
    heapSizesExec = [str(i+1) + "g" for i in range(1, int(memorySize))]

    algorithms = ["+UseParallelGC", "+UseG1GC"]
    instanceCount = 0

    for hd in heapSizesDriver: # 4
        for he in heapSizesExec: # 4
            #for th in threshold:
            for alg in algorithms: # 2
                args = "--driver-memory " + hd + " --executor-memory " + he +\
                       " --conf spark.executor.extraJavaOptions=-XX:" + alg
                c = str(instanceCount)
                instanceCount += 1
                yield (True, args, c)


def GenerateGrid(queriesToRun, memorySize):
    driverOptions = r"${DRIVER_OPTIONS}"
    executorOptions = r"${EXECUTOR_OPTIONS}"
    testPath = r"${TEST_PATH}"
    outPath = r"${OUT_PATH}"

    for i, config in enumerate(GetNextConfig(memorySize)):
        _, args, optName = config
        for qName in queriesToRun:
            print("bin/spark-sql", end=" ")
            print(driverOptions, end=" ")
            print(executorOptions, end=" ")
            print("-database TPCDS", end=" ")
            print("-f " + testPath + "query" + qName + ".sql", end=" ")
            print(args, end=" ")
            print("> " + outPath + "q_" + qName + "config_" + optName + ".txt" + " 2>&1 ", end="" )
            print("\n")

        # num of configs = memorySize * memorySize * num algorithms
        progress = int((i+1) / (memorySize * memorySize * 2) * 100)
        print("echo -ne " + str(progress) + "% complete \\\r"),
    print




if __name__ == "__main__":
    args = parser.parse_args()

    queriesToRun = ["01", "02", "03"]


    testPath = '"/home/goodlad/bench/spark-tpc-ds-performance-test/MasterQueries/"'
    outPath = '"/home/goodlad/bench/spark-tpc-ds-performance-test/output/"'
    sparkSqlPath = '"/usr/local/spark/"'


    driverOptions='" --driver-java-options -Dlog4j.configuration=file:///home/goodlad/bench/spark-tpc-ds-performance-test/MasterQueries/log4j.properties"'
    executorOptions='" --num-executors 1 --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=-Dlog4j.configuration=file:///home/goodlad/bench/spark-tpc-ds-performance-test/MasterQueries/log4j.properties --conf spark.sql.crossJoin.enabled=true"'


    print("#!/bin/bash")

    print("\n\n\n")

    print(r"TEST_PATH=" + testPath)
    print(r"OUT_PATH=" + outPath)
    print()
    print(r"DRIVER_OPTIONS=" + driverOptions)
    print(r"EXECUTOR_OPTIONS=" + executorOptions)
    print("\n\n\n")

    print("cd " + sparkSqlPath)

    print("\n\n\n")

    GenerateGrid(queriesToRun, args.ram)





