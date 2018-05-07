from configparser import ConfigParser
from elasticsearch import Elasticsearch


# 读取 config.ini 配置项
def config():
    cfg = ConfigParser()
    cfg.read('core/config.ini')

    return {'mysql':
                {
                    'host': cfg.get('mysql', 'host'),
                    'port': cfg.get('mysql', 'port'),
                    'user': cfg.get('mysql', 'user'),
                    'passwd': cfg.get('mysql', 'passwd'),
                    'charset': cfg.get('mysql', 'charset'),
                },
            'elasticsearch':
                {
                    'host': cfg.get('elasticsearch', 'host'),
                    'port': cfg.get('elasticsearch', 'port'),
                    'index': cfg.get('elasticsearch', 'index'),
                    'type': cfg.get('elasticsearch', 'type'),
                },
            }


def es():
    esclient = Elasticsearch(['localhost:9200'])
    response = esclient.search(
        index='observer',
        body={
            "query": {
                "match": {
                    "title": "儿童"
                }
            }
        }
    )
    print(response)


def main():
    


if __name__ == '__main__':
    print(main())
