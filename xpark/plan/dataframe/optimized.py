from collections import defaultdict
import logging

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
        return '<rule: %s\n%s>' % (self.__class__.__name__, self.rule_str.strip())

    def transform_path(self, path, g):
        return path, {}

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
        all_path_stats = []
        if paths:
            _LOG.info('[%s] paths matched rule:%s', len(paths), self)
            for path in paths:
                new_path, stats = self.transform_path(path, g)
                if path != new_path:
                    has_changed = True
                    g = self.replace_path(g, path, new_path)
                    all_path_stats.append(stats)
        return g, has_changed, all_path_stats

    @staticmethod
    def apply_all(g):
        all_rule_stats = defaultdict(list)
        for rule_class in rule_classes:
            i = 0
            while True:
                _LOG.info('Checking rule:[%s] iteration:[%s]', rule_class, i)
                g, has_changed, all_path_stats = rule_class().apply(g)
                if has_changed:
                    all_rule_stats[rule_class].append(all_path_stats)
                else:
                    break
                i += 1
        return g, all_rule_stats


class PruneSerialization(OptimizationRule):
    rule_str = '''
    MATCH (start: {is_pure_compute: "True"})-->(n1:Serializechunkop)-->(n2:Deserializechunkop)-->(end: {is_pure_compute: "True"})
    RETURN start, n1, n2, end
    '''

    def transform_path(self, path, g):
        start, n1, n2, end = path
        new_path = [start, end]
        stats = {'old_path': path, 'new_path': new_path}
        return new_path, stats


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
        new_path = [first_op, new_read_op] + path[2:-2] + [last_op]
        stats = {'old_schema': read_op.schema, 'new_schema': new_schema, 'dataset': read_op.dataset}
        return new_path, stats


class PushDownImplicitSelect(OptimizationRule):
    rule_str = '''
    MATCH (read:Readdatasetop)-[*]->(last)
    RETURN read, nodes, last
    '''
    # '''
    # MATCH p=(read:Readdatasetop)-[*]->(last)
    # WHERE any(node in nodes(p) WHERE labels(node) IN ["Filterchunkop"])
    # RETURN nodes(p)
    # '''

    def transform_path(self, path, g):
        return path, {}


class PushDownCount(OptimizationRule):
    rule_str = '''
    MATCH (start:Physicalstartop)-->(read:Readdatasetop)-->(count:Countchunkop)-->(sum:Sumop)
    RETURN start, read, count, sum
    '''
    # '''
    # MATCH p=(read:Readdatasetop)-[*]->(count:Countchunkop)-->(sum:Sumop)
    # WHERE none(node in nodes(p) WHERE labels(node) IN ["Filterchunkop"])
    # RETURN start, read, count, sum
    # '''

    def transform_path(self, path, g):
        from xpark.dataset.files import FileList
        from xpark.plan.dataframe.physical import ReadDatasetCountOp

        start_op = path[0]
        read_op = path[1]
        sum_op = path[-1]
        if read_op.dataset.file_list.file_type == FileList.FILE_TYPE_PARQUET:
            new_read_op = ReadDatasetCountOp(dataset=read_op.dataset, schema=read_op.schema,
                                             plan=read_op.plan, part_id=read_op.part_id)
            new_path = [start_op, new_read_op, sum_op]
            stats = {'dataset': read_op.dataset}
            return new_path, stats
        else:
            return path, {}


class PruneChunks(OptimizationRule):
    rule_str = '''
    MATCH (first)-->(read:Readdatasetop)-[*]->(last: {is_terminal: "True"})
    RETURN first, read, nodes, last
    '''
    # '''
    # MATCH p=(read:Readdatasetop)-[*]->(filter:Filterchunkop)-[*]->(last)
    # RETURN nodes(p)
    # '''

    def transform_path(self, path, g):
        from xpark.dataset import FileDataset
        from xpark.dataset.files import FileList
        from xpark.dataset.utils import pq_prune_chunks_min_max
        from xpark.plan.dataframe.physical import FilterChunkOp

        first_op = path[0]
        read_op = path[1]
        dataset = read_op.dataset
        filter_ops = [op for op in path if isinstance(op, FilterChunkOp)]
        combined_filter = None
        if filter_ops and isinstance(dataset, FileDataset) and dataset.file_list.file_type == FileList.FILE_TYPE_PARQUET:
            for op in filter_ops:
                op_filter = op.ac_kwargs['expr']
                if combined_filter is None:
                    combined_filter = op_filter
                else:
                    combined_filter = combined_filter & op_filter

            to_keep = set(pq_prune_chunks_min_max(read_op.dataset, combined_filter))
            if read_op.part_id not in to_keep:
                new_path = [first_op]
                stats = {'skip': read_op.part_id}
                return new_path, stats
        return path, {}


class UseIndexForFilter(OptimizationRule):
    rule_str = '''
    MATCH (first)-->(read:Readdatasetop)-[*]->(last: {is_terminal: "True"})
    RETURN first, read, nodes, last
    '''
    # '''
    # MATCH p=(read:Readdatasetop)-[*]->(filter:Filterchunkop)-[*]->(last)
    # RETURN nodes(p)
    # '''

    def transform_path(self, path, g):
        from xpark.dataset.tables import Table
        from xpark.plan.dataframe.physical import PostIndexFilterChunkOp, ReadIndexFilterChunkOp, FilterChunkOp

        # import pdb;pdb.set_trace()
        first_op = path[0]
        read_op = path[1]
        table = read_op.dataset
        filter_ops = [(i, op) for i, op in enumerate(path) if isinstance(op, FilterChunkOp)]
        if filter_ops and isinstance(table, Table):
            op_idx, filter_op = filter_ops[0]
            expr = filter_op.ac_kwargs['expr']
            indexed_cols, index_expr, extra_cols, extra_expr = table.extract_cols_from_expr(expr, filter_op.schema)
            if indexed_cols:
                new_path = [first_op]
                if extra_cols:
                    new_path.append(read_op)
                new_path.extend(path[2:op_idx])
                new_index_filter_op = ReadIndexFilterChunkOp(
                    plan=filter_op.plan,
                    schema=filter_op.schema,
                    part_id=filter_op.part_id,
                    table=table,
                    expr=index_expr,
                    augment_cols=indexed_cols + extra_cols
                )
                new_path.append(new_index_filter_op)
                if extra_expr:
                    new_filter_op = PostIndexFilterChunkOp(
                        plan=filter_op.plan, schema=filter_op.schema,
                        part_id=filter_op.part_id, expr=extra_expr
                    )
                    new_path.append(new_filter_op)
                new_path.extend(path[op_idx + 1:])
                stats = {
                    'indexed_cols': indexed_cols,
                    'index_expr': index_expr,
                    'extra_cols': extra_cols,
                    'extra_expr': extra_expr
                }
                return new_path, stats
        return path, {}


rule_classes = [
    PruneSerialization,
    PushDownImplicitSelect,
    PushDownSelect,
    PruneChunks,
    UseIndexForFilter,
    PushDownCount,
]


class OptimizedPlan(BaseOptimizedPlan):
    stats = None
