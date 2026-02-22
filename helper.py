from collections import defaultdict
from fastapi import HTTPException, status
from typing import List, Dict, Any
from pydantic import BaseModel

UNVISITED = -1
VISITING = 0
VISITED = 1

class NodeModel(BaseModel):
    id: str
    type: str
    position: Dict[str, float]
    data: Dict[str, Any]

class EdgeModel(BaseModel):
    id: str
    source: str
    target: str
    sourceHandle: str | None = None
    targetHandle: str | None = None

class PipelineModel(BaseModel):
    nodes: List[NodeModel]
    edges: List[EdgeModel]


def directed_dfs(node, flag, graph):
    # if node is already visited then it is cyclic
    if flag[node] == VISITING:
        return False
    if flag[node] == VISITED:
        return True
    # marking as visited
    flag[node] = VISITING

    for neighbour in graph[node]:
        if not directed_dfs(neighbour, flag, graph):
            return False
    
    # node is visited, so backtracking
    flag[node]=VISITED
    return True

def is_dag_func(nodes, edges):
    print(nodes, edges, "nodes, edges--")
    graph = defaultdict(list)
    for edge in edges:
        graph[edge.source].append(edge.target)
    # -1 denotes node is not visited yet
    #  0 denotes node is visited and is in stack
    #  1 denotes node is visited and popped out of the stack
    flag = {node.id: UNVISITED for node in nodes}

    for node in nodes:
        if flag[node.id] == UNVISITED:
            if not directed_dfs(node.id, flag, graph):
                return False
    return True

def validate_dag(pipeline: PipelineModel):
    nodes = pipeline.nodes
    edges = pipeline.edges
    if not is_dag_func(nodes, edges):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline must be a DAG (cycle detected)"
        )

    return pipeline
