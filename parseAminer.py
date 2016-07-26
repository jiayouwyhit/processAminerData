# coding=utf-8
import json
import datetime
import pymongo
from pymongo import MongoClient
from bson import json_util


def readTextDataFromFile(_file_path):
    with open(_file_path, 'r') as content_file:
        content = content_file.read()
        return content


def readDataFromFileLineByLine(_file_path):
    all_papers = []
    one_paper = {'title': '', 'authors': [], 'year': '', 'venue': '', 'index': '', 'references': [], 'citations': [],
                 'abstract': ''}
    with open(_file_path, 'r') as content_file:
        line_counter = 0
        most_recent_mark = ''
        for one_line in content_file:
            line_counter = line_counter + 1
            one_line = one_line.rstrip('\n')

            # get the most recent mark
            if len(one_line) != 0 and one_line[0] == '#':
                most_recent_mark = one_line[:2]

            if len(one_line) == 0:
                flag = True
                for attr in one_paper:  # check if extra empty row exists
                    flag = flag and (len(one_paper[attr]) == 0)
                if flag:
                    print '========================Error: Wrong empty row!!!!!!!===================' + str(line_counter)
                else:
                    all_papers.append(one_paper)
                    one_paper = {'title': '', 'authors': [], 'year': '', 'venue': '', 'index': '', 'references': [],
                                 'citations': [],
                                 'abstract': ''}
            elif one_line[:2] == '#*':
                one_paper['title'] = one_line[2:]
            elif one_line[:2] == '#@':
                one_paper['authors'] = one_line[2:].split(', ')
            elif one_line[:2] == '#t':
                one_paper['year'] = one_line[2:]
            elif one_line[:2] == '#c':
                one_paper['venue'] = one_line[2:]
            elif one_line[:2] == '#i':
                one_paper['index'] = one_line[6:]
            elif one_line[:2] == '#!':
                one_paper['abstract'] = one_line[2:]
            elif one_line[:2] == '#%':
                one_paper['references'].append(one_line[2:])
            else:
                if most_recent_mark == '#!':
                    one_paper['abstract'] = one_paper['abstract'] + ' ' + one_line
                else:
                    print '========================Error: no match!!!!!!!===================' + str(line_counter)

        print 'line_counter: ' + str(line_counter)
        return all_papers


def list2Dict(_data_list):
    data_dict = {}
    for i in range(0, len(_data_list)):
        idx = _data_list[i]['index']
        data_dict[idx] = _data_list[i]

    return data_dict


def updateCitations(_data_list, _data_dict):
    for i in range(0, len(_data_list)):
        for j in range(0, len(_data_list[i]['references'])):
            refer_id = _data_list[i]['references'][j]
            cur_id = _data_list[i]['index']
            _data_dict[refer_id]['citations'].append(cur_id)
    return _data_dict


def getPapersOfResearcher(_data_dict, _researcher):
    paper_list = []
    found = False
    for attr, value in _data_dict.iteritems():
        found = False
        for i in range(0, len(value['authors'])):
            for j in range(0, len(_researcher)):
                if value['authors'][i] == _researcher[j]:
                    paper_list.append(value)
                    found = True
                    break
            if found:
                break
    return paper_list


def saveList2JsonFile(_file_path, _data_list):
    with open(_file_path, 'w') as f:
        f.write(json_util.dumps(_data_list)) #!!!!The correct way to tranform JSON-unserializable data to JSON-serializable data


def insertData2DB(_data_dict, _paper_collection):
    for attr, paper in _data_dict.iteritems():
        result = _paper_collection.insert_one(paper)
        # print result


def processRawData2DB(_raw_data_path, _paper_collection):
    print '=======================start processing data===================='
    print datetime.datetime.now()

    all_data_list = readDataFromFileLineByLine(_raw_data_path)
    # all_data_list = readDataFromFileLineByLine('./test.txt')
    print '=======================finish reading files===================='
    print datetime.datetime.now()

    # data list to dictionary
    all_data_dict = list2Dict(all_data_list)
    print '=======================finish transform list to dict===================='
    print datetime.datetime.now()

    # update citations
    all_data_dict = updateCitations(all_data_list, all_data_dict)
    print '=======================update citation data===================='
    print datetime.datetime.now()

    # insert the result to mongodb
    insertData2DB(all_data_dict, _paper_collection)


if __name__ == "__main__":
    # process the data and input it to db
    client = MongoClient('localhost', 27017)
    db = client['aminerDatabase']
    paper_collection = db['allPapers']

    working_mode = 'PROCESSING_DATA'
    if working_mode == 'PROCESSING_DATA':
        print '=======================Insert data to collection===================='
        print datetime.datetime.now()
        db.drop_collection('allPapers') #drop the collection in which we store our data
        processRawData2DB('./citation-acm-v8.txt', paper_collection)

    else:
        # find the papers of one researcher from mongoDB
        researchers = [  # at most 4 elements for each row
            ['Jeffrey Heer', 'J. Heer', 'J Heer'],
            ['Klaus Mueller', 'K. Mueller', 'K Mueller'],
            ['Tamara Munzner', 'T. Munzner', 'T Munzner'],
            ['Torsten Möller', 'T. Möller', 'T Möller'],
            ['Martin Wattenberg', 'M. Wattenberg', 'M Wattenberg', 'Martin M. Wattenberg'],
            ['Huamin Qu', 'H. Qu'],
            ['Christos Faloutsos', 'C. Faloutsos', 'C Faloutsos'],
            ['Jiawei Han', 'J. Han', 'J Han']
        ]

        for i in range(0, len(researchers)):
            # paper_list = getPapersOfResearcher(all_data_dict, researchers[i])
            if len(researchers[i]) == 1:
                paper_list = paper_collection.find({"$or": [{'authors': researchers[i][0]}]})\
                    .sort([("year",pymongo.ASCENDING)])
            elif len(researchers[i]) == 2:
                paper_list = paper_collection.find({"$or": [{'authors': researchers[i][0]}, {'authors': researchers[i][1]}]}) \
                    .sort([("year", pymongo.ASCENDING)])
            elif len(researchers[i]) == 3:
                paper_list = paper_collection.find({"$or": [{'authors': researchers[i][0]}, {'authors': researchers[i][1]}, {'authors': researchers[i][2]}]}) \
                    .sort([("year", pymongo.ASCENDING)])
            elif len(researchers[i]) == 4:
                paper_list = paper_collection.find({"$or": [{'authors': researchers[i][0]}, {'authors': researchers[i][1]}, {'authors': researchers[i][2]}, {'authors': researchers[i][3]} ]}) \
                    .sort([("year", pymongo.ASCENDING)])

            file_name = 'researcher_text/' + researchers[i][0] + '.json'
            saveList2JsonFile(file_name, paper_list)

    print '=======================Job is done===================='
    print datetime.datetime.now()
