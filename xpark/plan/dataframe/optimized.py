import logging

import itertools
import networkx as nx
from networkx.classes.function import add_path
from python_cypher import python_cypher
from xpark.plan.base import BaseOptimizedPlan
from xpark.utils.iter import take_pairs

_LOG = logging.getLogger(__name__)
PARSER = python_cypher.CypherToNetworkx()


class OptimizationRule(object):
    rule_str = None

    def __init__(self):
        self.rule = PARSER.parse(' '.join(self.rule_str.split()))

    def __repr__(self):
        return self.rule_str.strip()

    def transform_path(self, path, g):
        return g

    def replace_path(self, g, path, new_path):
        g = g.copy()
        paths = list(take_pairs(path))
        g.remove_edges_from(paths)
        add_path(g, new_path)
        g.remove_nodes_from(list(nx.isolates(g)))
        return g

    def apply(self, g):
        paths = list(PARSER.yield_return_values(g, self.rule))
        has_changed = False
        if paths:
            _LOG.info('[%s] paths matched rule:%s', len(paths), self)
            for path in paths:
                new_path = self.transform_path(path, g)
                if path != new_path:
                    has_changed = True
                    g = self.replace_path(g, path, new_path)
        return g, has_changed

    @staticmethod
    def apply_all(g):
        for rule_class in rule_classes:
            i = itertools.count()
            while i:
                _LOG.info('Checking rule:[%s] iteration:[%s]', rule_class, i)
                g, has_changed = rule_class().apply(g)
                if not has_changed:
                    break
        return g


class PruneSerialization(OptimizationRule):
    rule_str = '''
    MATCH (start: {is_pure_compute: "True"})-->(n1:Serializechunkop)-->(n2:Deserializechunkop)-->(end: {is_pure_compute: "True"})
    RETURN start, n1, n2, end
    '''

    def transform_path(self, path, g):
        start, n1, n2, end = path
        return [start, end]


class PushDownSelect(OptimizationRule):
    rule_str = '''
    MATCH (start:Readdatasetop)-[*]->(end:Selectchunkop)
    RETURN start, end
    '''


class PushDownCount(OptimizationRule):
    rule_str = '''
    MATCH (start:Readdatasetop)-[*]->(end:Countchunkop)
    RETURN start, end
    '''


class PushDownFilter(OptimizationRule):
    rule_str = '''
    MATCH (start:Readdatasetop)-[*]->(end:Filterchunkop)
    RETURN start, end
    '''


rule_classes = [
    PruneSerialization,
    PushDownSelect,
    PushDownCount,
    PushDownFilter,
]


class OptimizedPlan(BaseOptimizedPlan):
    pass
