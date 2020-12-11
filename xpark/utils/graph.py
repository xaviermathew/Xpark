import networkx as nx


class DiGraph(nx.DiGraph):
    def add_node(self, node_for_adding, **attr):
        attr['class'] = node_for_adding.__class__.__name__.title()
        attr['is_pure_compute'] = str(node_for_adding.is_pure_compute)
        attr['is_terminal'] = str(node_for_adding.is_terminal)
        return super(__class__, self).add_node(node_for_adding, **attr)

    def add_edge(self, u_of_edge, v_of_edge, **attr):
        self.add_node(u_of_edge)
        self.add_node(v_of_edge)
        super(__class__, self).add_edge(u_of_edge, v_of_edge, **attr)
