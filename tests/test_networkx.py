import networkx as nx

graph = nx.DiGraph()
graph.add_node("first")
graph.add_node("second")
graph.add_node("second-b")
graph.add_node("third")

graph.add_edge("first", "second")
# graph.add_edge("second", "first")

print(nx.is_directed_acyclic_graph(graph))
