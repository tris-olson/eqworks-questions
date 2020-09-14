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

markedList = [0] * len(relationsList)
# where 0 is unmarked, 1 is marked
path = []

#actually need to rethink graph traversal to properly solve this q
def DFS(graph, mList, edge, marking):
    if marking == 0:
        marking = 1
        for x in range(0, len(relationsList)):
            if relationsList[x[1]] == edge[0]:
                DFS(graph, mList, relationsList[x], mList[x])

