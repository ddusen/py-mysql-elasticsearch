import os, sys
sys.path.append(os.getcwd())

from configparser import ConfigParser

from utils.mysql import (query, query_one, save, )
from utils.string import (date_to_str, )


# 读取 config.ini 配置项
def read_config():
    cfg = ConfigParser()
    cfg.read('core/config.ini')

    mysql_conf = {
                    'host': cfg.get('mysql', 'host'),
                    'port': cfg.get('mysql', 'port'),
                    'user': cfg.get('mysql', 'user'),
                    'passwd': cfg.get('mysql', 'passwd'),
                    'charset': cfg.get('mysql', 'charset'),
                    'db': cfg.get('mysql', 'db'),
                }
    elastic_conf = {
                    'host': cfg.get('elastic', 'host'),
                    'port': cfg.get('elastic', 'port'),
                    'index': cfg.get('elastic', 'index'),
                    'type': cfg.get('elastic', 'type'),
                }

    return {'mysql': mysql_conf, 'elastic': elastic_conf}


# 业务相关方法：获取 category
def get_categories(mysql_conf, article_guid):
    sql = '''SELECT `base_category`.`name` FROM `base_category` WHERE `base_category`.`id` IN ( 
                    SELECT `base_articlecategory`.`category_id` FROM `base_articlecategory` WHERE `base_articlecategory`.`article_id` = %s 
                ) '''
    categories = query(sql=sql, db_config=mysql_conf, list1=(article_guid, ))

    return list(map(lambda x : {'name': x[0]}, categories))


# 业务相关方法：获取 area
def get_areas(mysql_conf, article_guid):
    sql = '''SELECT `base_area`.`name` FROM `base_area` WHERE `base_area`.`id` IN ( 
                    SELECT `base_articlearea`.`area_id` FROM `base_articlearea` WHERE `base_articlearea`.`article_id` = %s 
                ) '''
    areas = query(sql=sql, db_config=mysql_conf, list1=(article_guid, ))

    return list(map(lambda x : {'name': x[0]}, areas))


# 业务相关方法：获取 article total
def get_article_total(mysql_conf):
    sql = 'SELECT COUNT(*) FROM `base_article`'
    total = query_one(sql=sql, db_config=mysql_conf)[0]

    return total


# 业务相关方法：获取 article
def get_articles(mysql_conf, start, length):
    sql = 'SELECT `guid`, `title`, `url`, `pubtime`, `source`, `score` FROM `base_article` WHERE `status` <> -1 ORDER BY `pubtime` DESC LIMIT %s, %s'
    articles = query(sql=sql, db_config=mysql_conf, list1=(start, length, ))

    return articles


# 业务相关方法：sql data to doc
def data_to_doc(mysql_conf, data):
    '''sql data like this:
    (
        '湖南省质监局大力推动认证认可工作服务地方经济成效显著',
        'http://www.cqn.com.cn/zgzlb/content/2018-05/04/content_5736939.htm',
        datetime.datetime(2018, 5, 4, 0, 0),
        '中国质量新闻网', 
        0
    )
    '''
    guid = data[0]
    title = data[1] 
    url = data[2] 
    pubtime = date_to_str(data[3]) 
    source = data[4] 
    score = data[5] 
    areas = get_areas(mysql_conf, guid)
    categories = get_categories(mysql_conf, guid)

    '''doc model like this:
    {
        "source": "中国新闻网",
        "title": "对儿童安全构成威胁 指尖陀螺被欧盟列为危险品",
        "pubtime": "2018-03-14 00:00:00",
        "url": "http://dw.chinanews.com/chinanews/content.jsp?id=8467281&classify=zw&pageSize=6&language=chs",
        "score": 2,
        "category": [
            { "name": "xxx" },
            { "name": "xxx" }
        ],
        "area": [
            { "name": "咸宁" },
            { "name": "孝感" }
        ]
    }
    '''
    doc = {
            "title": title,
            "url": url, 
            "pubtime": pubtime, 
            "source": source, 
            "score": score, 
            "category": categories,
            "area": areas
        }

    return doc

# 验证文档是否存在
def exists_by_doc_id(esclient, elastic, doc_id):
    return esclient.exists(
            index=elastic['index'],
            doc_type=elastic['type'],
            id=doc_id,
        )

