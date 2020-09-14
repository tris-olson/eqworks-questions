"""
just doing a quick lil graph traversal
start node: 73
end node: 36

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


def graphTraversal(goal, edges, visited, stack):
    if visited[taskIDs.index(goal)] == 0:
        visited[taskIDs.index(goal)] = 1
        for x in range(0, len(edges)):
            if edges[x][1] == goal:
                graphTraversal(edges[x][0], edges, visited, stack)
        stack.append(goal)
    return stack

def findPath(start, goal, nodes, edges):
    visitedList = [0] * len(nodes)  # where 0 is unvisited, 1 is visited
    goalDependencies = graphTraversal(goal, edges, visitedList, [])
    visitedList = [0] * len(nodes)
    startDependencies = graphTraversal(start, edges, visitedList, [])
    
    finalPath = []
    for x in goalDependencies:
        if x not in startDependencies:
            finalPath.append(x)
        if x == start:
            finalPath.append(x)
    return finalPath

    
print(findPath('73', '36', taskIDs, relationsList))