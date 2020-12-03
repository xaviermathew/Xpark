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
        return path

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
    MATCH (first)-->(read:Readdatasetop)-[*]->(select:Selectchunkop)-->(last)
    RETURN first, read, nodes, select, last
    '''

    def transform_path(self, path, g):
        from xpark.plan.dataframe.physical import ReadDatasetOp

        first_op = path[0]
        read_op = path[1]
        select_op = path[-2]
        last_op = path[-1]
        new_schema = {}
        new_read_op = ReadDatasetOp(dataset=read_op.dataset, schema=new_schema,
                                    plan=read_op.plan, part_id=read_op.part_id)
        for col in select_op.ac_kwargs['cols']:
            new_schema[col] = read_op.schema[col]
        return [first_op, new_read_op] + path[2:-2] + [last_op]


class PushDownCount(OptimizationRule):
    rule_str = '''
    MATCH (start:Physicalstartop)-->(read:Readdatasetop)-->(count:Countchunkop)-->(sum:Sumop)
    RETURN start, read, count, sum
    '''

    def transform_path(self, path, g):
        from xpark.plan.dataframe.physical import ReadDatasetCountOp

        start_op = path[0]
        read_op = path[1]
        sum_op = path[-1]
        new_read_op = ReadDatasetCountOp(dataset=read_op.dataset, schema=read_op.schema,
                                         plan=read_op.plan, part_id=read_op.part_id)
        return [start_op, new_read_op, sum_op]


class PushDownFilter(OptimizationRule):
    rule_str = '''
    MATCH (read:Readdatasetop)-[*]->(filter:Filterchunkop)
    RETURN read, filter
    '''


rule_classes = [
    PruneSerialization,
    PushDownSelect,
    # PushDownFilter,
    PushDownCount,
]


class OptimizedPlan(BaseOptimizedPlan):
    pass
