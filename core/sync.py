from elasticsearch import Elasticsearch

from process import (read_config, get_articles, get_article_total, get_areas, 
                    get_categories, )
from utils.string import (date_to_str, )


class Sync:

    def __init__(self):
        config = read_config()
        self.mysql = config['mysql']
        self.elastic = config['elastic']

    #基于 SQL 语句的全量同步    
    def full_sql(self):
        total = get_article_total(self.mysql,)
        start = 0
        length = 100
        while start <= total:
            articles = get_articles(self.mysql, start, length)
            for article in articles:
                guid = article[0] 
                title = article[1] 
                url = article[2] 
                pubtime = date_to_str(article[3]) 
                source = article[4] 
                score = article[5] 
                areas = get_areas(self.mysql, guid)
                categories = get_categories(self.mysql, guid)

                doc = {
                        "title": title,
                        "url": url, 
                        "pubtime": pubtime, 
                        "source": source, 
                        "score": score, 
                        "category": categories,
                        "area": areas
                    }

                self.elastic_save(guid, doc)

            start += length

    # elastic save
    def elastic_save(self, guid, doc):
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


if __name__ == '__main__':
    Sync().full_sql()
