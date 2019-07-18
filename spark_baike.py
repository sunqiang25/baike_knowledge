import zipfile
import io
from hdfs.client import Client
from pyspark import SparkContext, SparkConf
from BaikeGraph import BaikeGraph
from parsing import html_parse
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

def main(content):
    """
    content is a tuple (filename, filecontent)
    """
    # with open(in_path) as f:
    redi = redis.StrictRedis(host="10.243.55.67", port=6379)
    filename = content[0].split('/')[-1]
    file_content = content[1]
    #file_content=content
    # line = f.read()
    # hdfs_client = Client('http://10.243.55.67:50070/', root='/')
    # outpath = os.path.join("/bres", middle_path)
    # l = []
    ret = {}
    ret["name"] = ""
    ret["alias"] = ""
    # index get id
    index = filename.split('__')[0]
    
    # index = in_path.split(split_char)[0].split('/')[-1]
    ret["vid"] = index
    redis_result = redi.get(str(index)).decode("utf-8").split("@@")
    if len(redis_result) == 1:
        ret["name"] = redis_result[-1]
    elif len(redis_result) == 2:
        ret["name"] = redis_result[0]
        ret['alias'] = redis_result[1]
    try:
        obj = json.loads(file_content, encoding="utf8")
        #            url, content, status = obj["url"], obj["content"], obj["status"]
        url, content = obj["url"], obj["content"]
        html_handler = html_parse(content, url)
        handler = BaikeGraph()
        #            ret["url"] = url
        content = html_handler.html_clean()
        # parse title tag box
        ret["iid"] = html_handler.parse_itemId()
        #html_handler.create_rel(index,ret["iid"])
        boxes = html_handler.parse_box_new()
        desc = html_handler.parse_desc_new()
        titles = html_handler.parse_title_new()
        tags = html_handler.parse_tag_new()
        dict_final = ChainMap(boxes, desc, ret, titles, tags)
        handler.create_baike_node(dict(dict_final))
        html_handler.parse_polysemantic()

    except Exception as e:
        print('exception in main @@@@@@@@@@@@@@@@@2222')
        print(e)
        pass
    return

def zip_extract(x):
    in_memory_data = io.BytesIO(x[1])
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    files = [i for i in file_obj.namelist()]
    print(files)
    for file in files:
        content = file_obj.open(file).read().decode(encoding='utf-8')
        main((file, content))
if __name__ == '__main__':
#    hdfs_dir =  Client('http://10.243.55.67:50070/', root='/')
#    baike_zip = hdfs_dir.list('/baike/')
    conf = SparkConf().setAppName('baike_parsing').setMaster('spark://10.243.55.67:7077')
    conf.set("spark.default.parallelism", "10000")
    conf.set("spark.driver.maxResultSize", "3g")
    sc = SparkContext.getOrCreate(conf)
    count = 0
    #dir_list = ['rst1000000',"rst2000000","rst10000000"]
    #for dir_name in dir_list:
    for root,dirs,files in os.walk("/baike/txtdir/"):
        for bd in files:
            #path = "hdfs://10.243.55.67:9000/baike/" + bd + "/"
            path = "file://"+os.path.join(root,bd)
            rdd = sc.textFile(path, minPartitions=10000)
            #result_rdd = rdd.map(main)
            res = rdd.foreach(main)
            count+=1
            print("finished*********************************************************",count)
