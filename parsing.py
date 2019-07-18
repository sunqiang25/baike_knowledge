#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: sunqiang
"""
import json, codecs, requests, re
import re, redis
from time import sleep
import os,urllib
from bs4 import BeautifulSoup
from collections import ChainMap
from BaikeGraph import BaikeGraph
class html_parse:
    def __init__(self, html, url):
        self.html = html
        self.url = url
        self.host = "https://baike.baidu.com"
        self.soup = BeautifulSoup(self.html)
        self.redi = redis.StrictRedis(host="10.243.55.67", port=6379)

    def html_clean(self):
        '''
        remove
    
        eg:
        <sup class="sup--normal" data-sup="1">[1]</sup>
        <a class="sup-anchor" name="ref_[1]_1000042">&nbsp;</a>
    
        :param html:
        :param url:
        :return:
        '''
        html = re.sub(r'<sup\sclass=\"sup--normal\"\sdata-sup=*.+[\s\S].+</sup>', "", self.html)
        # return re.sub(r'<a\sclass=\"sup-anchor\"\sname=\"*.+</a>', "", html)
        return html

    def parse_title_new(self):
        '''
        obtain title
        eg:
            'baidubaike'
    
        :param html:
        :param url:
        :return:
        '''          
        ret = {}
        title = self.soup.select(".lemmaWgt-lemmaTitle-title > h1")
        if len(title) > 0:
            ret['title'] = title[0].text.strip()

        sub_tt = self.soup.select(".lemmaWgt-lemmaTitle-title > h2")
        if len(sub_tt) > 0:
            ret["sub_title"] = sub_tt[0].text.strip()
        return ret

    def get_html_baikeId(self, url):
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
            "Host": "baike.baidu.com"}
        baike_id = ""
        proxies = {"http": "http://10.244.2.4:8099", "https": "http://10.244.2.4:8099"}
        try:
            response = requests.get(url, timeout=1, verify=False, headers=headers, proxies=proxies,allow_redirects=True)
            response.encoding = "utf-8"
            html = response.text
            ano_url = response.url
            url_item = urllib.parse.unquote(ano_url[29:]).replace("#viewPageContent","")

            url_item = re.sub('(&fromid=[0-9]*$)','',url_item)
            url_item = re.sub('\/[0-9]*\?fromtitle','',url_item)

            url_item = re.sub('\/[0-9]*$','',url_item)
            html_clean = "".join(html.split())
            #id_list = re.findall("(?<=setGlobal\(\{lemmaId:)(.*?)(?=\,newLemmaId)", html_clean)
            itemId_list = re.findall("setGlobal\(\{lemmaId:(.*?)\,newLemmaId:(.*?)\,subLemmaId", html_clean)
            if itemId_list:
                for i in itemId_list:
                    item_id_str = "".join(i[1]).strip().replace('\"', "")
                    item_id = item_id_str if item_id_str.isdigit() else ""
                    baike_id_str = "".join(i[0]).strip().replace('\"', "")
                    baike_id = baike_id_str if baike_id_str.isdigit() else ""  
            file_name = str(baike_id) +'__'+str(item_id) +'__' + url_item + '.txt'
            return baike_id, html,file_name
        except Exception as e:
            print(e)
            pass
        return baike_id, "",""
    def parse_itemId(self):
        item_id = ""
        html_clean = "".join(self.html.split())
        itemId_list = re.findall("setGlobal\(\{lemmaId:(.*?)\,newLemmaId:(.*?)\,subLemmaId", html_clean)
        if itemId_list:
            for i in itemId_list:
                item_id_str = "".join(i[1]).strip().replace('\"', "")
                item_id = item_id_str if item_id_str.isdigit() else ""
                if item_id: break
        return item_id
    
    def parse_polysemantic(self):
        semantics=self.soup.select(".polysemantList-wrapper a")
        if semantics:
            for semantic in semantics:
                uri = semantic.get("href","")
                if uri:
                    url=self.host+uri
                    baike_id, html,file_name = self.get_html_baikeId(url)
                    ret = {}
                    ret["vid"] = baike_id
                    redis_result = self.redi.get(baike_id).decode("utf-8").split("@@")
                    if len(redis_result) == 1:
                        ret["name"] = redis_result[-1]
                    elif len(redis_result) == 2:
                        ret["name"] = redis_result[0]
                        ret['alias'] = redis_result[1]
                    htm_handler = html_parse(html, url)
                    ret["iid"] = htm_handler.parse_itemId()
                    handler = BaikeGraph()
                    #if 1:
                    boxes = htm_handler.parse_box_new()
                    desc = htm_handler.parse_desc_new()
                    titles = htm_handler.parse_title_new()
                    tags = htm_handler.parse_tag_new()
                    dict_final = ChainMap(boxes, desc, ret, titles, tags)
                    handler.create_baike_node(dict(dict_final))
                    htm_handler.create_rel(baike_id,ret["iid"])
                    if file_name:
                        data = {"url":url,"neo4jProperty":dict(dict_final),"content":html}
                        with codecs.open("/baike/polysemantic/"+file_name,"w","utf-8") as f:
                            f.write(json.dumps(data))                          
                        
    def parse_box_new(self):        
        ret = {}

        names = self.soup.select(".basicInfo-item.name")
        values = self.soup.select(".basicInfo-item.value")
        if len(names) == len(values):
            for i in range(len(names)):
                name = "".join(names[i].text.strip().split())
                value = values[i].text.strip()
                ret[name] = ",".join(self.split_str2list(value))
        return ret

    def parse_tag(self):      
        ret = []

        tags = self.soup.select("#open-tag-item >  span")
        for item in tags:
            if item.text != "":
                ret.append([item.text.strip(), ('' if item.a is None else item.a['href'])])
        return ret

    @staticmethod
    def split_str2list(s):
        pattern = r"[\/\s\.\!,$%^*]+|[()?【】，。、……&*（）]+"
        if s.replace(" ", "").isalpha():
            l = [s]
        else:
            l = re.split(pattern, s)
        return l

    def parse_tag_new(self):       
        ret = {}
        tags_list = []
        tags = self.soup.select("#open-tag-item >  span")
        for item in tags:
            if item.text != "":
                tags_list.append(item.text.strip())
                # ret.append([item.text.strip(), ('' if item.a is None else item.a['href'])])
        ret['tags'] = ",".join(tags_list)
        return ret

    def parse_desc_new(self):       
        '''
            lemma-summary
        '''
        ret = {}
        if self.soup.select(".lemma-summary"):
            desc = self.soup.select(".lemma-summary")[0].text.strip()
            ret['desc'] = desc
        return ret

    def parse_index(self,html=None):
        pass
    
    def create_rel(self, start_vid,start_iid):
        ret = {}

        names = self.soup.select(".basicInfo-item.name")
        values = self.soup.select(".basicInfo-item.value")
        if len(names) == len(values):
            for i in range(len(names)):
                name = "".join(names[i].text.strip().split())
                a_list = values[i].find_all("a")
                if a_list:
                    for i in a_list:
                        i_url = i.get("href")
                        i_name = i.text
                        if i_url:
                            full_url = self.host + i_url
                            baike_id, html_new,file_name = self.get_html_baikeId(full_url)
                            if baike_id:
                                #new_url = self.host + "/view/%s.htm" % (baike_id)
                                new_url = full_url
                                handler = BaikeGraph()
                                new_html_parse = html_parse(html_new,new_url)
                                iid = new_html_parse.parse_itemId()                                 
                                #node_exsit = handler.nodeExist(baike_id)
                                node_exsit = handler.nodeExist("Baike6",iid)
                                rel_exsit = handler.relExist(start_iid,name,iid)
                                if node_exsit:
                                    if not rel_exsit:
                                        handler.create_relationship(start_vid,start_iid,baike_id,iid,name)
                                elif html_new:
                                    try:
                                        ret = {}
                                        ret["vid"] = baike_id
                                        ret["iid"] = iid
                                        redis_result = self.redi.get(baike_id).decode("utf-8").split("@@")
                                        if len(redis_result) == 1:
                                            ret["name"] = redis_result[-1]
                                            ret["alias"] = ""
                                        elif len(redis_result) == 2:
                                            ret["name"] = redis_result[0]
                                            ret['alias'] = redis_result[1]                                        
                
                                        boxes = new_html_parse.parse_box_new()
                                        desc = new_html_parse.parse_desc_new()
                                        titles = new_html_parse.parse_title_new()
                                        tags = new_html_parse.parse_tag_new()
                                        dict_final = ChainMap(boxes, desc, ret, titles, tags)
                                        handler.create_baike_node(dict(dict_final))
                                        if not rel_exsit:
                                            handler.create_relationship(start_vid,start_iid,baike_id,iid,name)  
                                            if file_name:
                                                data = {"url":new_url,"neo4jProperty":dict(dict_final),"content":html_new}
                                                if os.path.exists("/baike/extra/"+file_name):
                                                    with codecs.open("/baike/extra/"+file_name,"w","utf-8") as f:
                                                        f.write(json.dumps(data))
                                    except:
                                        pass




