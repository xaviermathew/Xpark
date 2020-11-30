import logging

from python_cypher import python_cypher
from xpark.plan.base import BaseOptimizedPlan

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

    def apply(self, g):
        paths = list(PARSER.yield_return_values(g, self.rule))
        if paths:
            _LOG.info('[%s] paths matched optimization rule:%s', len(paths), self)
            for path in paths:
                g = self.transform_path(path, g)
        return g

    @staticmethod
    def apply_all(g):
        for rule_class in rule_classes:
            g = rule_class().apply(g)
        return g


class PruneSerialization(OptimizationRule):
    rule_str = '''
    MATCH (start: {is_pure_compute: "True"})-->(n1:Serializechunkop)-->(n2:Deserializechunkop)-->(end: {is_pure_compute: "True"})
    RETURN start, end
    '''


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
