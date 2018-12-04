import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--ram', dest='ram', default=4, help='number of gigabytes you have for RAM/want to use')


def GetNextConfig(memorySize):
    # return true if there is another option, the args to pass and a name for a file
    heapSizesDriver = (str(i) + "g" for i in range(int(memorySize)))
    heapSizesExec = (str(i) + "g" for i in range(int(memorySize)))
    # heapSizes = ["1g", "2g", "3g", "4g"]
    # threshold = ["1", "2", "3", "4"]
    algorithms = ["+UseParallelGC", "+UseG1GC"]
    instanceCount = 0

    for hd in heapSizesDriver:  # 4
        for he in heapSizesExec:  # 4
            # for th in threshold:
            for alg in algorithms:  # 2
                args = "--driver-memory " + hd + " --executor-memory " + he + " -XX:" + alg
                c = str(instanceCount)
                instanceCount += 1
                yield (True, args, c)


def GenerateGrid(queriesToRun, memorySize):
    driverOptions = r"${DRIVER_OPTIONS}"
    executorOptions = r"${EXECUTOR_OPTIONS}"
    testPath = r"${TEST_PATH}"
    outPath = r"${OUT_PATH}"

    for config in GetNextConfig(memorySize):
        _, args, optName = config
        for qName in queriesToRun:
            print("bin/spark-sql " + driverOptions + " " + executorOptions + " " + "-database TPCDS "),
            print("-f " + testPath + "query" + qName + ".sql " + args + " "),
            print("> " + outPath + "q_" + qName + "config_" + optName + ".txt" + " 2>&1 ")

if __name__ == "__main__":
    args = parser.parse_args()

    queriesToRun = ["01", "02", "03"]

    executorOptions = '" --num-executors 1 --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=-Dlog4j.configuration=file:///mnt/c/Users/JohnG/Desktop/cs239/spark-tpc-ds-performance-test/log4j.properties --conf spark.sql.crossJoin.enabled=true"'
    driverOptions = '" --driver-java-options -Dlog4j.configuration=file:///mnt/c/Users/JohnG/Desktop/cs239/spark-tpc-ds-performance-test/log4j.properties"'

    testPath = '"/mnt/c/Users/JohnG/Desktop/Queries/"'
    outPath = '"/mnt/c/Users/JohnG/Desktop/cs239/"'
    sparkSqlPath = '"/usr/local/spark/"'

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





