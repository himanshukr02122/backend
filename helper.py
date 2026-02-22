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

# using Kahn's algorithm
def isDag(nodes, edges):
    n = len(nodes)
    indegree = [0] * n
    adj = [[] for _ in range(n)]
    node_index = {node.id: i for i, node in enumerate(nodes)}
    for edge in edges:
        source = node_index[edge.source]
        target = node_index[edge.target]
        print(source, target, "source, target", edge.source, "edge.source", edge.target, "edge.target")
        adj[source].append(target)
        indegree[target] += 1
    
    queue, count = [], 0 # maintaining queue for nodes with indegree 0
    for i in range(n):
        if indegree[i]==0:
            queue.append(i)
    
    while queue:
        node = queue.pop(0)
        count += 1

        for neighbor in adj[node]:
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                queue.append(neighbor)
    return count == n

def validate_dag(pipeline: PipelineModel):
    nodes = pipeline.nodes
    edges = pipeline.edges
    if not isDag(nodes, edges):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline must be a DAG (cycle detected)"
        )

    return pipeline
