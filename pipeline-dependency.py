# question 4b
# start node: 73
# end node: 36

import re

# load in list of edges
file = open("/tmp/data/relations.txt")  
edges_list = []
for line in file:
    edges_list = edges_list + [re.findall('[0-9]+', line)]  
file.close()

# load in task list
taskIDs = []
file = open("/tmp/data/task_ids.txt")
for line in file:
    taskIDs = taskIDs + re.findall('[0-9]+', line)   
file.close()

# finds a topological sorting for the graph above the goal node
def graph_traversal(goal, nodes, edges, visited, stack):
    if visited[nodes.index(goal)] == 0:  # if current node hasn't been visited yet
        visited[nodes.index(goal)] = 1   # update visited status
        for x in range(0, len(edges)):
            if edges[x][1] == goal:      # for all prereq nodes connected to current node
                # recursively call graphTraversal to find all earlier prereq nodes
                graph_traversal(edges[x][0], nodes, edges, visited, stack)
        # appending to stack here preserves topological sorting
        stack.append(goal)
    return stack

# computes the target path taking into account start and goal nodes
def find_path(start, goal, nodes, edges):
    visited_list = [0] * len(nodes)  # where 0 is unvisited, 1 is visited
    goal_dependencies = graph_traversal(goal, nodes, edges, visited_list, [])
    visited_list = [0] * len(nodes)
    start_dependencies = graph_traversal(start, nodes, edges, visited_list, [])
    
    # remove any prereqs that have been already satisfied given the start node
    final_path = []
    for x in goal_dependencies:
        if x not in start_dependencies:  # remove any prereqs already satisfied
            final_path.append(x)         # given the start node
        if x == start:                  
            final_path.append(x)         # keep start node if it is a prereq of the goal
    return final_path

    
print(find_path('73', '36', taskIDs, edges_list))