# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 19:50:49 2020

just doing a quick lil graph traversal
start node: 73
end node: 36

@author: Tricia
"""

import re

file = open("relations.txt")  
 
relationsList = []

for line in file:
    relationsList = relationsList + [re.findall('[0-9]+', line)]
    
file.close()

taskIDs = []

file = open("task_ids.txt")

for line in file:
    taskIDs = taskIDs + re.findall('[0-9]+', line)
    
file.close()


markedList = [0] * len(taskIDs)
# where 0 is unmarked, 1 is marked
path = []

def graphTraversal(graph, mList, node, end, maybeList):
    if mList[taskIDs.index(node)] == 0:
        mList[taskIDs.index(node)] = 1
        checkedList = []
        # this just pops everything above stream of anything else on the list, this is the issue
        for x in range(0, len(relationsList)):
            if relationsList[x][1] == node:
                checkedList = checkedList + [relationsList[x][0], graphTraversal(graph, mList, relationsList[x][0], end, maybeList)]
        for x in range(0, len(relationsList)):   
            if relationsList[x][0] == node:
                if relationsList[x][1] == end:
                    checkedList = checkedList + [end, maybeList]
                else:
                    maybeList = maybeList + [relationsList[x][1]]
                    checkedList = checkedList + [graphTraversal(graph, mList, relationsList[x][1], end, maybeList)]
        return checkedList
    
def removeNone(l):
    newList = []
    for x in l:
        if x != None:
            newList = newList + [x]
    return newList
    
def flatten(l):
    flat_list = []
    for x in l:
        flat_list.append(x)
    return flat_list

# testing but this probably won't work right away
emptyList = []
print(flatten(removeNone(graphTraversal(relationsList, markedList, '73', '36', emptyList))))