import os, sys
sys.path.append(os.getcwd())

from configparser import (ConfigParser, RawConfigParser, )

from utils.mysql import (query, query_one, save, )
from utils.string import (date_to_str, )


# 读取 config.ini 配置项
def read_config():
    cfg = ConfigParser()
    cfg.read('core/config.ini')

    mysql_conf = {
        'host': cfg.get('mysql', 'host'),
        'port': cfg.getint('mysql', 'port'),
        'user': cfg.get('mysql', 'user'),
        'passwd': cfg.get('mysql', 'passwd'),
        'charset': cfg.get('mysql', 'charset'),
        'db': cfg.get('mysql', 'db'),
    }
    elastic_conf = {
        'init': cfg.get('elastic', 'init'),
        'host': cfg.get('elastic', 'host'),
        'port': cfg.getint('elastic', 'port'),
        'index': cfg.get('elastic', 'index'),
        'type': cfg.get('elastic', 'type'),
    }
    
    is_null = lambda x : None if not x else x
    is_list = lambda x : None if not x else eval(x)

    binlog_conf = {
        'server_id': cfg.getint('mysql_binlog', 'server_id'),
        'blocking': cfg.getboolean('mysql_binlog', 'blocking'),
        'log_file': is_null(cfg.get('mysql_binlog', 'log_file')),
        'log_pos': is_null(cfg.get('mysql_binlog', 'log_pos')),
        'only_schemas': is_list(cfg.get('mysql_binlog', 'only_schemas')),
        'only_tables': is_list(cfg.get('mysql_binlog', 'only_tables')),
    }

    return {'mysql': mysql_conf, 'elastic': elastic_conf, 'binlog': binlog_conf, }

# 写入 config.ini 配置项
def write_config(section, key, value):
    cfg = RawConfigParser()
    cfg.read('core/config.ini')
    if section not in cfg.sections():
        cfg.add_section(section)

    cfg.set(section, key, value)

    with open('core/config.ini', 'w') as f:
        cfg.write(f)

# 初始化 elastic doc types
def init_elastic(flag):
    if 'True' == flag:
        # 修改初始化为 False
        write_config('elastic', 'init', 'False')
        # 执行初始化命令
        return '''self._elastic(doc={ "mappings": {
                                    "article": {
                                        "properties": {
                                            "title": {
                                                "type": "text"
                                            },
                                            "url": {
                                                "type": "keyword"
                                            },
                                            "pubtime": {
                                                "type": "date",
                                                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                                            },
                                            "source": {
                                                "type": "text"
                                            },
                                            "score": {
                                                "type": "long"
                                            },
                                            "invalid_keyword": {
                                                "type": "text"
                                            },
                                            "risk_keyword": {
                                                "type": "text"
                                            }
                                        }
                                    }
                                }
                            }, option="init")'''
    else:
        return '1 + 1'

# 业务相关方法：获取 categories
def get_categories(mysql_conf, article_guid):
    sql = '''SELECT `base_category`.`name` FROM `base_category` WHERE `base_category`.`id` IN ( 
                    SELECT `base_articlecategory`.`category_id` FROM `base_articlecategory` WHERE `base_articlecategory`.`article_id` = %s 
                ) '''
    categories = query(sql=sql, db_config=mysql_conf, list1=(article_guid, ))

    return list(map(lambda x : {'name': x[0]}, categories))


# 业务相关方法：获取 areas
def get_areas(mysql_conf, article_guid):
    sql = '''SELECT `base_area`.`name` FROM `base_area` WHERE `base_area`.`id` IN ( 
                    SELECT `base_articlearea`.`area_id` FROM `base_articlearea` WHERE `base_articlearea`.`article_id` = %s 
                ) '''
    areas = query(sql=sql, db_config=mysql_conf, list1=(article_guid, ))

    return list(map(lambda x : {'name': x[0]}, areas))


# 业务相关方法：获取 articles total
def get_article_total(mysql_conf):
    sql = 'SELECT COUNT(*) FROM `base_article`'
    total = query_one(sql=sql, db_config=mysql_conf)[0]

    return total


# 业务相关方法：获取 articles
def get_articles(mysql_conf, start, length):
    sql = 'SELECT `guid`, `title`, `url`, `pubtime`, `source`, `score`, `risk_keyword`, `invalid_keyword` FROM `base_article` WHERE `status` <> -1 ORDER BY `pubtime` DESC LIMIT %s, %s'
    articles = query(sql=sql, db_config=mysql_conf, list1=(start, length, ))

    return articles


# 业务相关方法：获取 article
def get_article(mysql_conf, guid):
    sql = 'SELECT `guid`, `title`, `url`, `pubtime`, `source`, `score`, `risk_keyword`, `invalid_keyword` FROM `base_article` WHERE `guid` = %s'
    article = query_one(sql=sql, db_config=mysql_conf, list1=(guid, ))

    return article


# 业务相关方法：sql data to doc
def sqldata_to_doc(mysql_conf, data):
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
    risk_keyword = data[6] 
    invalid_keyword = data[7] 
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
            "risk_keyword": risk_keyword, 
            "invalid_keyword": invalid_keyword, 
            "category": categories,
            "area": areas
        }

    return doc


# 业务相关方法：binlog data to doc
def bindata_to_doc(mysql_conf, data):
    '''bin data like this:
    {
        'source': '安徽日报',
         'title': '老年手机、移动电源和童装不合格率超六成',
         'status': 0,
         'url': 'http://epaper.anhuinews.com/html/ahrb/20180319/article_3649201.shtml',
         'invalid_keyword': '',
         'score': 0,
         'pubtime': datetime.datetime(2018, 3, 19, 0, 0),
         'guid': 'b832087238263b9199e7c92285287951',
         'risk_keyword': ''
     }
    '''
    guid = data['guid']
    pubtime = date_to_str(data['pubtime']) 
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
            "title": data['title'],
            "url": data['url'], 
            "pubtime": pubtime, 
            "source": data['source'], 
            "score": data['score'], 
            "risk_keyword": data['risk_keyword'], 
            "invalid_keyword": data['invalid_keyword'], 
            "category": categories,
            "area": areas
        }

    return doc


# 业务相关方法：解析 binlog 删除操作
def bin_delete(table, values):
    if 'base_article' == table:
        doc_id = values['guid']
        return 'self._elastic("%s", doc={}, option="delete")' % doc_id

    elif 'base_articlearea' == table:
        doc_id = values['article_id']
        return 'self._elastic("%s", doc=sqldata_to_doc(self.mysql, get_article(self.mysql, "%s")), option="update")' % (doc_id, doc_id, )

    elif 'base_articlecategory' == table:
        doc_id = values['article_id']
        return 'self._elastic("%s", doc=sqldata_to_doc(self.mysql, get_article(self.mysql, "%s")), option="update")' % (doc_id, doc_id, )

    else:
        return '1 + 1'


# 业务相关方法：解析 binlog 新增操作
def bin_create(table, values):
    if 'base_article' == table:
        doc_id = values['guid']
        return 'self._elastic("%s", doc=bindata_to_doc(self.mysql, values), option="create")' % doc_id

    elif 'base_articlearea' == table:
        doc_id = values['article_id']
        return 'self._elastic("%s", doc=sqldata_to_doc(self.mysql, get_article(self.mysql, "%s")), option="update")' % (doc_id, doc_id, )

    elif 'base_articlecategory' == table:
        doc_id = values['article_id']
        return 'self._elastic("%s", doc=sqldata_to_doc(self.mysql, get_article(self.mysql, "%s")), option="update")' % (doc_id, doc_id, )

    else:
        return '1 + 1'


# 业务相关方法：解析 binlog 更新操作
def bin_update(table, values):
    if 'base_article' == table:
        doc_id = values['guid']
        return 'self._elastic("%s", doc=bindata_to_doc(self.mysql, values), option="update")' % doc_id

    elif 'base_articlearea' == table:
        doc_id = values['article_id']
        return 'self._elastic("%s", doc=sqldata_to_doc(self.mysql, get_article(self.mysql, "%s")), option="update")' % (doc_id, doc_id, )

    elif 'base_articlecategory' == table:
        doc_id = values['article_id']
        return 'self._elastic("%s", doc=sqldata_to_doc(self.mysql, get_article(self.mysql, "%s")), option="update")' % (doc_id, doc_id, )

    else:
        return '1 + 1'