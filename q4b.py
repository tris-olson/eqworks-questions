# question 4b
# start node: 73
# end node: 36

import re

# load in list of edges
file = open("relations.txt")  
edgesList = []
for line in file:
    edgesList = edgesList + [re.findall('[0-9]+', line)]  
file.close()

# load in task list
taskIDs = []
file = open("task_ids.txt")
for line in file:
    taskIDs = taskIDs + re.findall('[0-9]+', line)   
file.close()

# finds a topological sorting for the graph above the goal node
def graphTraversal(goal, nodes, edges, visited, stack):
    if visited[nodes.index(goal)] == 0:  # if current node hasn't been visited yet
        visited[nodes.index(goal)] = 1   # update visited status
        for x in range(0, len(edges)):
            if edges[x][1] == goal:      # for all prereq nodes connected to current node
                # recursively call graphTraversal to find all earlier prereq nodes
                graphTraversal(edges[x][0], nodes, edges, visited, stack)
        # appending to stack here preserves topological sorting
        stack.append(goal)
    return stack

# computes the target path taking into account start and goal nodes
def findPath(start, goal, nodes, edges):
    visitedList = [0] * len(nodes)  # where 0 is unvisited, 1 is visited
    goalDependencies = graphTraversal(goal, nodes, edges, visitedList, [])
    visitedList = [0] * len(nodes)
    startDependencies = graphTraversal(start, nodes, edges, visitedList, [])
    
    # remove any prereqs that have been already satisfied given the start node
    finalPath = []
    for x in goalDependencies:
        if x not in startDependencies:  # remove any prereqs already satisfied
            finalPath.append(x)         # given the start node
        if x == start:                  
            finalPath.append(x)         # keep start node if it is a prereq of the goal
    return finalPath

    
print(findPath('73', '36', taskIDs, edgesList))