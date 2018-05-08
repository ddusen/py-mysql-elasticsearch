import os, sys
sys.path.append(os.getcwd())

from configparser import ConfigParser

from utils.mysql import (query, query_one, save, )


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