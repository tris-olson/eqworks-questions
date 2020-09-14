# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 19:50:49 2020

just doing a quick lil graph traversal
start node: 73
end node: 36

@author: Tricia
"""

import re
import collections

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


def graphTraversal(goal, edges, seenList):
    path = ['0']
    if seenList[taskIDs.index(goal)] == 0:
        seenList[taskIDs.index(goal)] = 1
        path.append(goal)
        for x in range(0, len(edges)):
            if edges[x][1] == goal:
                path.append(graphTraversal(edges[x][0], edges, seenList))
    return path
    

def flatten(l):
    newList = []
    for x in l:
        if isinstance(l, collections.Iterable) and not isinstance(x, str):
            newList.extend(flatten(x))
        else:
            newList.append(x)
    return newList

def removeZeros(l):
    newList = []
    for x in l:
        if x != '0':
            newList.append(x)
    return newList

def findPath(start, goal, nodes, edges):
    markedList = [0] * len(nodes)
    # where 0 is unmarked, 1 is marked
    goalDependencies = graphTraversal(goal, edges, markedList)
    goalDependencies = removeZeros(flatten(goalDependencies))
    markedList = [0] * len(nodes)
    startDependencies = graphTraversal(start, edges, markedList)
    startDependencies = removeZeros(flatten(startDependencies))
    
    finalPath = []
    for x in goalDependencies:
        if x not in startDependencies:
            finalPath.append(x)
        if x == start:
            finalPath.append(x)
    
    return finalPath

    
print(findPath('73', '36', taskIDs, relationsList))

                

"""
    
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
"""