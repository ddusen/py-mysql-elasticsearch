import os, sys
sys.path.append(os.getcwd())

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import (NotFoundError, ConflictError, )

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, 
                                        )

from process import (read_config, get_articles, get_article_total,
                    data_to_doc, write_config, )
from utils.logger import (Logger, )


class Sync:

    def __init__(self):
        # inital config
        config = read_config()
        self.mysql = config['mysql']
        self.elastic = config['elastic']
        self.sqlbinlog = config['sqlbinlog']

        # inital logging
        self.logger = Logger()

    #基于 SQL 语句的全量同步    
    def _full_sql(self):
        self.logger.record('Starting：based full sql...')
        
        total = get_article_total(self.mysql,)
        start = 0
        length = 100
        while start <= total:
            queryset = get_articles(self.mysql, start, length)
            for q in queryset:
                # 数据库ID -> 文档ID
                guid = q[0]
                # format data
                doc = data_to_doc(self.mysql, q)
                # elastic save
                self.elastic_save_or_update(guid, doc)

            start += length

        self.logger.record('Ending：based full sql.')

    # 基于 binlog 的增量实时同步
    def _binlog(self):
        self.logger.record('Starting：based binlog...')

        stream = BinLogStreamReader(connection_settings=self.mysql,
                                    server_id=self.sqlbinlog['server_id'],
                                    blocking=self.sqlbinlog['blocking'],
                                    log_file=self.sqlbinlog['log_file'],
                                    log_pos=self.sqlbinlog['log_pos'],
                                    only_schemas=self.sqlbinlog['only_schemas'],
                                    only_tables=self.sqlbinlog['only_tables'],
                                    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, ],
                                    )

        for binlogevent in stream:

            # record log file and log pos
            # if isinstance(binlogevent, RotateEvent):
            #     write_config('mysql_binlog', 'log_file', stream.log_file)
            #     write_config('mysql_binlog', 'log_pos', stream.log_pos)
            #     continue

            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    print(row['values'])
                    # if binlogevent.table == 'self.master':
                    #     rv = {
                    #         'action': 'delete',
                    #         'doc': row['values']
                    #     }
                    # else:
                    #     rv = {
                    #         'action': 'update',
                    #         'doc': {k: row['values'][k] if self.id_key and self.id_key == k else None for k in row['values']}
                    #     }
                # elif isinstance(binlogevent, UpdateRowsEvent):
                #     rv = {
                #         'action': 'update',
                #         'doc': row['after_values']
                #     }
                # elif isinstance(binlogevent, WriteRowsEvent):
                #     if binlogevent.table == 'self.master':
                #         rv = {
                #                 'action': 'create',
                #                 'doc': row['values']
                #             }
                #     else:
                #         rv = {
                #                 'action': 'update',
                #                 'doc': row['values']
                #             }
        stream.close()

        # write_config('test', '1', '2')
        self.logger.record('Ending：based binlog.')

    # elastic 
    def _elastic(self, doc_id, doc={}, option='create'):
        """
        option: 
            create: 若文档已存在，则不执行任何操作。 若文档不存在，则直接创建。
            update: 若文档已存在，则直接更新。 若文档不存在，则不执行任何操作。
            delete: 若文档已存在，则直接删除。若文档不存在，则不执行任何操作。
        """
        esclient = Elasticsearch([self.elastic])
        
        status = 'Success !'

        if 'create' == option:
            try:
                esclient.create(
                        index=self.elastic['index'],
                        doc_type=self.elastic['type'],
                        id=doc_id,
                        body=doc,
                    )
            except ConflictError:
                status = 'Fail(existsd) !'

        if 'update' == option:
            try:
                esclient.update(
                        index=self.elastic['index'],
                        doc_type=self.elastic['type'],
                        id=doc_id,
                        body={'doc':doc},
                    )
            except NotFoundError:
                status = 'Fail(not existsd) !'

        if 'delete' == option:
            try:
                esclient.delete(
                        index=self.elastic['index'],
                        doc_type=self.elastic['type'],
                        id=doc_id,
                    )
            except NotFoundError:
                status = 'Fail(not existsd) !'

        self.logger.record('Sync@%s < %s-%s-%s > %s' % (option, self.elastic['index'], self.elastic['type'], doc_id, status, ))


if __name__ == '__main__':
    sync = Sync()
    # sync.full_sql()
    # sync.binlog()
    doc = {
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
    guid = '528eb39855e876852c1f6371a82ea634'
    sync._elastic(guid, doc=doc, option='update')
