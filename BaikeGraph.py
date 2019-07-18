#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: sunqiang
"""
from py2neo import Graph, Node, NodeMatcher,RelationshipMatcher
class BaikeGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.cur_dir = cur_dir
        self.g = Graph(
            host="10.37.2.248",
            http_port=7474,
            user="neo4j",
            password="1qaz@WSX")
        self.matcher = NodeMatcher(self.g)
        #self.rel_matcher = RelationshipMatcher(self.g)
    def nodeExist(self,lable,iid):
        dict_id = {"iid":iid}
        m = self.matcher.match(lable, **dict_id).first()
        if m is None:
            return False
        else:
            return True
    
    def nodeExist_new(self, lable,iid):
        query = "MATCH (n:%s) where n.iid='%s' RETURN n"%(lable,iid)
        try:
            m = self.g.run(query).data()
            if m:
                return True
            else:
                return False
        except Exception as e:
            print(e)
            return False

    def create_baike_node(self, baike_infos):
        node = Node("Baike6", **baike_infos)
        if not self.g.exists(node):
            self.g.create(node)
        return

    def relExist(self,start_iid,rel,end_iid):
        query = "MATCH p=(n:Baike4{vid:'%s'})-[r:`%s`]->(m:Baike4{vid:'%s'}) return p"%(start_iid,rel,end_iid)
        try:
            m=self.g.run(query).data()
            if m:
                return True
            else:
                return False
        except Exception as e:
            print(e)
            return False        

    def create_relationship(self, start_node_vid, start_node_iid,end_node_vid, end_node_iid,rel, start_node="Baike6", end_node="Baike6"):

        query = "match(p:%s),(q:%s) where p.vid='%s' and p.iid='%s' and q.vid='%s' and q.iid='%s' merge (p)-[rel:%s{name:'%s'}]->(q)" % (
            start_node, end_node, start_node_vid, start_node_iid, end_node_vid,end_node_iid,rel, rel)
        try:
            self.g.run(query)
        except Exception as e:
            print(e)
        return

