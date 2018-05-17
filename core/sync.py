import os, sys
sys.path.append(os.getcwd())

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import (NotFoundError, ConflictError, RequestError, )

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, 
                                        )

from process import (read_config, get_articles, get_article_total,
                    sqldata_to_doc, write_config, bin_delete, 
                    bindata_to_doc, bin_create, bin_update, 
                    init_elastic, )
from utils.logger import (Logger, )


class Sync:

    def __init__(self):
        # inital config
        config = read_config()
        self.mysql = config['mysql']
        self.elastic = config['elastic']
        self.binlog = config['binlog']

        # inital logging
        self.logger = Logger()

        # inital elastic doc types
        eval(init_elastic(self.elastic['init']))


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
                doc = sqldata_to_doc(self.mysql, q)
                # elastic save
                self._elastic(guid, doc, option='create')

            start += length

        self.logger.record('Ending：based full sql.')

    # 基于 binlog 的增量实时同步
    def _binlog(self):
        self.logger.record('Starting：based binlog...')

        stream = BinLogStreamReader(connection_settings=self.mysql,
                                    server_id=self.binlog['server_id'],
                                    blocking=self.binlog['blocking'],
                                    log_file=self.binlog['log_file'],
                                    log_pos=self.binlog['log_pos'],
                                    only_schemas=self.binlog['only_schemas'],
                                    only_tables=self.binlog['only_tables'],
                                    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, ],
                                    )
        for binlogevent in stream:

            #record log file and log pos
            write_config('mysql_binlog', 'log_file', stream.log_file)
            write_config('mysql_binlog', 'log_pos', stream.log_pos)

            for row in binlogevent.rows:

                values = row.get('values')
                values = row.get('after_values') if not values else values

                if isinstance(binlogevent, DeleteRowsEvent):
                    eval(bin_delete(binlogevent.table, values))

                elif isinstance(binlogevent, WriteRowsEvent):
                    eval(bin_create(binlogevent.table, values))

                elif isinstance(binlogevent, UpdateRowsEvent):
                    eval(bin_update(binlogevent.table, values))

        stream.close()

        self.logger.record('Ending：based binlog.')

    # elastic 
    def _elastic(self, doc_id=None, doc={}, option='create'):
        """
        option: 
            init: 初始化文档结构。（当config.ini中的init为True时才会执行。）
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
                
        elif 'update' == option:
            try:
                esclient.update(
                        index=self.elastic['index'],
                        doc_type=self.elastic['type'],
                        id=doc_id,
                        body={'doc':doc},
                    )
            except NotFoundError:
                status = 'Fail(not existsd) !'

        elif 'delete' == option:
            try:
                esclient.delete(
                        index=self.elastic['index'],
                        doc_type=self.elastic['type'],
                        id=doc_id,
                    )
            except NotFoundError:
                status = 'Fail(not existsd) !'

        elif 'init' == option:
            try:
                IndicesClient(esclient).create(
                    index=self.elastic['index'],
                    body=doc,
                )
            except RequestError:
                status = 'Fail(existsd) !'

        self.logger.record('Sync@%s < %s-%s-%s > %s' % (option, self.elastic['index'], self.elastic['type'], doc_id, status, ))


if __name__ == '__main__':
    sync = Sync()
    # sync._full_sql()
    sync._binlog()
