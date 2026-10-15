import unittest
from types import SimpleNamespace
from pgclusterlib.runtime import Runtime


class MmrDataOnlyBootstrapTest(unittest.TestCase):
    def test_data_only_join_prepares_probe_before_join(self):
        members={'node1':{'node_name':'node1','streaming_cluster':'s1'},
                 'node2':{'node_name':'node2','streaming_cluster':'s2','join':{'synchronize_structure':'data-only'}}}
        config=SimpleNamespace(hosts={}, mmr_clusters={'group':{'database':'postgres','group_name':'g1','extensions':[],'members':members}},
            instance=lambda name:{'host_config':{'address':'127.0.0.1'},'port':15432 if name=='s1' else 25432})
        runtime=Runtime(config)
        calls=[]
        runtime._sync_fbase_regress_state=lambda *_:None
        runtime._progress=lambda _:None
        runtime.create_streaming=lambda *_:None
        runtime._primary=lambda name:name
        def sql(node,query,*args):
            calls.append((node,query))
            if 'SELECT count(*) FROM fdd.mmr_group' in query:return '1'
            if 'SELECT count(*)' in query:return '1'
            if 'show_node_info' in query:return 'node1|ACTIVE|ACTIVE|OK\nnode2|ACTIVE|ACTIVE|OK'
            if 'string_agg' in query:return 'node1,node2'
            return ''
        runtime._psql=sql
        runtime.create_mmr('group')
        prepare=next(i for i,(node,q) in enumerate(calls) if node=='s2' and 'CREATE TABLE IF NOT EXISTS pgcluster.mmr_probe' in q)
        join=next(i for i,(_,q) in enumerate(calls) if 'fdd.join_group' in q)
        self.assertLess(prepare,join)
        health_prepare=next(i for i,(node,q) in enumerate(calls) if node=='s2' and 'CREATE TABLE IF NOT EXISTS fbase_regress_mmr_health' in q)
        self.assertLess(health_prepare,join)
