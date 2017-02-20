#!/usr/bin/python -tt

import requests
import json
import re
import os
import shutil
import time
import csv
import sys
import logging
from datetime import datetime
from multiprocessing import Process
from collections import defaultdict,OrderedDict
import itertools

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#logging.disable(logging.INFO)

class Services:

    """
    This is a subclass of base class "Config" .
    Inherits parameters such as env,params,headers,authserver & url .
    This has additional functionality of generating cookies and thus modifies headers by appending cookie information to it.
    Also has method for executing web services.

    """

    '''def __init__(self,headers,params,authserver,url,env,data,query,queryNumber):
        super().__init__(params,headers,authserver,url)
        self.data = data
        self.query = query
        self.queryNumber = queryNumber'''

    def __init__(self,configList,params=None,headers=None):

        if params is None:
            params = {}

        if headers is None:
            headers = {}

        self.env = configList[0]
        self.params = params
        self.headers = headers
        self.params["appleId"] = configList[1]
        self.params["accountPassword"] = configList[2]
        self.params["appIdKey"] = configList[3]
        self.authserver = configList[4]
        self.headers["Content-Type"] = configList[5]
        self.headers["Host"] = configList[6]
        self.url = configList[7]
        with requests.session() as s:
            reqsessionObj = s.post(self.authserver,params = self.params)
            reqCookie = reqsessionObj.request.headers['Cookie']
            self.headers['Cookie'] = reqCookie

    def call_service(self,env,data,query,queryNumber):

        logging.debug('Env being executed {}'.format(env))
        logging.debug('query being executed {}'.format(query))

        url = self.url+query

        r = requests.post(url, data=json.dumps(data), headers=self.headers)

        with open(str(queryNumber)+'_'+str(env)+'_'+query+'.json','w') as fw:
            fw.write(json.dumps(r.json(),sort_keys=True,indent=4, separators=(',', ': ')))


class jsonFlatten:

    '''
    Below static function is called to convert json output to flat dictionary with values populated as a list.

    For ex:-

    Input json :

    "Key1": [
    {
        "Key2": {
            "Key3": [
                "xyz",
                "abc"
            ]

        }
    }
    ]

    Output:

    {
    "Key1_Key2_Key3": [
        "xyz",
        "abc"
    ]
    }

    '''

    @staticmethod
    def jsonParser(inputJson):

        finalDict={}
        finalList=[]
        seen = {}

        def jsonParserHelper(inputJson,name =''):

            #making it global because it's being referenced before assigned in else "finalDict[name] = finalList"
            global finalList

            if isinstance(inputJson,dict):
                for dictKey in inputJson:
                    finalList = []
                    jsonParserHelper(inputJson[dictKey],name+'_'+dictKey)
            elif isinstance(inputJson,list):
                if len(inputJson) == 0:
                    finalList.append('Empty')
                    finalDict[name] = finalList
                for eachItem in inputJson:
                    jsonParserHelper(eachItem,name)
            else:
                if name in seen.keys():
                    # seen[name] = seen[name] + 1
                    # name = name + '_' + str(seen[name])
                    finalDict[name].append(inputJson)
                    #print "Key already exists and it is {}".format(finalDict[name])
                else:
                    seen[name] = 0
                    finalList.append(inputJson)
                    finalDict[name] = finalList

        jsonParserHelper(inputJson)

        return finalDict


class CsvfileWriter:

    '''
    Takes dictionary as input and writes items into a CSV file.

    For ex:-

    Input dictionary:

    dict_data = {"1":["xyz"],"2":["abc","def"],"3":["zzz"]}

    Output: (CSV file)

    1,3,2
    xyz,zzz,abc
    ,,def

    '''

    def __init__(self,dictInput,fileName,maxLength=0):

        '''
        Creates a instance with following variables.
        dictInput,fileName & maxLength

        dictInput -> dictionary having values(list) of same length

        ex:-
            dict_data = {"1":["xyz",""],"2":["abc","def"],"3":["zzz",""]}

        fileName -> csv file name to be created.

        maxLength -> length of the list

        '''
        self.dictInput = dictInput
        self.maxLength = maxLength
        self.fileName = fileName

    @classmethod
    def list_padding(cls,dictInput,fileName):

        '''
        converts input dictionary having list (as values) of varying lenghts into constant length.
        Also returns class variables dictInput & maxLength

        Note:
        dictInput represents the dictionary after padding is applied.
        maxLength represents the length of the list(values in dictionary) having maximum number of items.

        Ex:-

        input dictionary:

        dict_data = {"1":["xyz"],"2":["abc","def"],"3":["zzz"]}

        output dictionary:

        dict_data = {"1":["xyz",""],"2":["abc","def"],"3":["zzz",""]}


        '''
        logging.info("......Class method variables assignment started......")
        cls.dictInput = dictInput
        cls.fileName = fileName
        logging.debug("dictInput is {}".format(cls.dictInput))
        logging.debug("file name is {}".format(cls.fileName))
        listValues =  dictInput.values()
        logging.debug("list values are {}".format(listValues))
        listValues.sort(key = lambda i: len(i))
        maxLength =  len(listValues[-1])
        logging.debug("maxLength is {}".format(maxLength))

        for i in listValues:
            while(len(i) < maxLength):
                i.append('')

        return cls(OrderedDict(sorted(dictInput.items())),fileName,maxLength)

    def write_to_csv(self):

        #os.chdir('/Users/Mahesh/Desktop/PythonWork/Automation/csv/')

        with open(self.fileName+'.csv','wb') as out_file:
            writer = csv.writer(out_file,dialect = 'excel')
            headers =  [k for k in self.dictInput]
            items = [self.dictInput[k] for k in self.dictInput]
            writer.writerow(headers)
            c = 0
            while (c < self.maxLength):
                writer.writerow([i[c] for i in items])
                c += 1

    @staticmethod
    def write_result_to_csv(objectList,fileName,headers=None,items=None):

        if headers is None:
            headers = []
        if items is None:
            items = []

        logging.debug(".....Execution of write_result_to_csv started.....")
        logging.debug("objectList is {}".format(objectList))

        dict_prod_modified = objectList[0].dictInput
        logging.debug("dict_prod_modified is {0}".format(dict_prod_modified))
        dict_uat_modified = objectList[1].dictInput
        logging.debug("dict_uat_modified is {0}".format(dict_uat_modified))

        keys = set(dict_prod_modified.keys()) | set(dict_uat_modified.keys())

        if isinstance(dict_prod_modified.values()[0], list):
            resultDict = {k: list(itertools.izip_longest(dict_prod_modified.get(k, [None]), dict_uat_modified.get(k, [None]))) for k in keys}
        else:
            resultDict = {k: [dict_prod_modified.get(k, None), dict_uat_modified.get(k, None)] for k in keys}

        logging.info("Consolidated dictionary of uat & prod is {}".format(resultDict))


        with open(fileName +'.csv','wb+') as out_file:
            writer = csv.writer(out_file,dialect ='excel')

            headersList = ['Metric Name', 'UAT Result', 'PROD Result', 'Validation Result']

            writer.writerow(headersList)

            for k,v in resultDict.items():

                for i in v:
                    itemsList = []
                    itemsList.append(k)
                    itemsList.append(i[0])
                    itemsList.append(i[1])

                    if i[0] == None or i[1] == None:
                        itemsList.append('NA')
                    elif i[0] == i[1]:
                        #print 'MATCHING',i[0],i[1]
                        itemsList.append('Matching')
                    else:
                        #print 'NOT MATCHING',i[0],i[1]
                        itemsList.append('Not Matching')

                    #print itemsList

                    writer.writerow(itemsList)


# Error handling

def exception_handling():
    print '{0}.{1},line: {2}'.format(sys.exc_info()[0],
                                        sys.exc_info()[1],
                                        sys.exc_info()[2].tb_lineno)




# folder management.

def folder_mgmnt(tobeDir):

    scriptDir = os.path.dirname(os.path.abspath(__file__)) # Parent directory of script.
    os.chdir(scriptDir) # Change directory to script path.

    if not os.path.isdir(tobeDir):
        os.mkdir(tobeDir) # Create a directory if it doesn't exist already.
    else:
        shutil.rmtree(os.path.join(scriptDir,tobeDir)) # Remove if file exists.
        os.mkdir(tobeDir) # Create a brand new directory again.

    return os.path.join(scriptDir,tobeDir)



# MAIN PROGRAM STARTS HERE.

if __name__ == '__main__':

    logging.info("************Hey there! Program just started****************")

    # Setting all Config details.

    envObjList = []

    with open('Config.csv', 'rU') as f:
        csvRead = csv.reader(f,delimiter=',',dialect='excel')
        header = 0
        for row in csvRead:

            if header != 0:

                logging.info('Creating environment object from Config class...')
                x = Services(row)
                logging.info('Storing environment object in a list...')
                envObjList.append(x)

                logging.debug('object {0}'.format(x))
                logging.debug('env {0}'.format(x.env))
                logging.debug('params {0}'.format(x.params))
                logging.debug('authserver {0}'.format(x.authserver))
                logging.debug('headers {0}'.format(x.headers))
                logging.debug('url {0}'.format(x.url))

            header += 1

    # Making webservice calls.

    workPath = folder_mgmnt('json')
    os.chdir(workPath)

    scriptDir = os.path.dirname(os.path.abspath(os.getcwd()))
    multiProcess = []
    with open(os.path.join(scriptDir,'input_requests.csv'),'rU') as f:
        csvRead = csv.reader(f,delimiter=',',dialect='excel')

        header = 0
        queryNumber = 0

        for row in csvRead:

            for envObject in envObjList:

                if header != 0:
                    logging.info('calling instance --> call_service...')
                    env = envObject.env
                    query = row[0]
                    data = json.loads(row[1])
                    logging.debug('Query being executed {0}'.format(query))
                    logging.debug('Request payload being executed {0}'.format(data))

                    p = Process(target = envObject.call_service, args = (env,data,query,queryNumber,))
                    multiProcess.append(p)
                    logging.debug('Execution of object {0}'.format(envObject))


            queryNumber += 1
            header += 1

        for p in multiProcess:
            p.start()

        for p in multiProcess:
            p.join()


    #  Writes json output from query jsons to CSV file.

    logging.info(".....Writing to individual CSV files started.....")

    dictJson = {}
    csvObjList = []

    #os.chdir('/Users/Mahesh/Desktop/PythonWork/Automation/json')
    scriptDir = os.path.dirname(os.path.abspath(os.getcwd())) # Parent directory of script.
    os.chdir(os.path.join(scriptDir,'json'))

    logging.debug(".......Parsing through json files to convert them into flat dictionaries......")

    for fileName in os.listdir(os.getcwd()):
        logging.debug("file name is {}".format(fileName))
        with open(fileName,'r') as f:
            fileContent = f.read()
            logging.debug("file name is {}".format(fileName))

            dictFileContent = json.loads(fileContent)

            try:
                dictJson['result'] = dictFileContent['result']
            except KeyError:
                if dictFileContent['status'] == 'error':
                    logging.error("{}".format(dictFileContent['error']['message']))
                    exception_handling()
                else:
                    exception_handling()
            except Exception:
                exception_handling()



            logging.info(".....calling the static method jsonParser.....")
            dictInput = jsonFlatten.jsonParser(dictJson['result'])
            logging.info(".....Json parsing is completed successfully.....")

            logging.info(".....Calling list_padding.....")

            try:
                cf = CsvfileWriter.list_padding(dictInput,fileName.split('.')[0])
            except Exception:
                logging.error('Error while creating a instance for {}'.format(fileName))
                exception_handling()

            logging.info(".....List padding is completed successfully.....")

            csvObjList.append(cf)

    logging.debug("file objects are {}".format(csvObjList))


    #  Writes json output from query jsons to CSV file.

    # workPath = folder_mgmnt('json')
    # os.chdir(workPath)

    multiProcess = []
    for csvObj in csvObjList:
        logging.debug("csv object name is {}".format(csvObj))
        logging.debug("csvObj dictInput is {}".format(csvObj.dictInput))
        logging.debug("csvObj fileName is {}".format(csvObj.fileName))

        logging.info(".....calling write_to_csv method for creation of {} .....".format(csvObj.fileName))
        p = Process(target = csvObj.write_to_csv)

        multiProcess.append(p)

    for p in multiProcess:
        p.start()

    for p in multiProcess:
        p.join()

    logging.info(".....write_to_csv is completed successfully.....")

    fileindexDict = {}
    fileIndexList = []

    '''
    output --> {'1': [<__main__.CsvfileWriter instance at 0x10fea4c20>, <__main__.CsvfileWriter instance at 0x10fea4ef0>], '2': [<__main__.CsvfileWriter instance at 0x10feb9320>, <__main__.CsvfileWriter instance at 0x10feb9a28>]}


    '''
    for csvObj in csvObjList:
        file_index = csvObj.fileName.split('_')[0]

        if file_index not in fileindexDict.keys():
            fileIndexList = []
            fileIndexList.append(csvObj)
            fileindexDict[file_index] = fileIndexList
        else:
            fileIndexList.append(csvObj)

    ro = re.compile(r'(\d+)_(production)_(.*)')

    #print "printing fileindexDict.items() {}".format(fileindexDict.items())

    #os.chdir('/Users/Mahesh/Desktop/PythonWork/Automation/result')
    workPath = folder_mgmnt('result')
    os.chdir(workPath)

    multiProcess = []
    for (k,v) in fileindexDict.items():
        fileName = v[0].fileName
        mo = ro.findall(fileName)
        fileName = k + '_' + mo[0][2]
        logging.info(".....calling write_result_to_csv method for creation of {} .....".format(fileName))
        p = Process(target = CsvfileWriter.write_result_to_csv(v,fileName,))
        multiProcess.append(p)

    for p in multiProcess:
        p.start()

    for p in multiProcess:
        p.join()

    logging.info("************End of the main program! see you again!****************")
