import os, sys
sys.path.append(os.getcwd())

from elasticsearch import Elasticsearch
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, 
                                        )

from process import (read_config, get_articles, get_article_total,
                    data_to_doc, exists_by_doc_id, write_config, )
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
    def full_sql(self):
        total = get_article_total(self.mysql,)
        start = 0
        length = 100
        while start <= total:
            queryset = get_articles(self.mysql, start, length)
            for q in queryset:
                # 唯一性标识
                guid = q[0]
                # format data
                doc = data_to_doc(self.mysql, q)
                # elastic save
                self.elastic_save(guid, doc)

            start += length

    # 基于 binlog 的增量实时同步
    def binlog(self):
        stream = BinLogStreamReader(connection_settings=self.mysql,
                                    server_id=self.sqlbinlog['server_id'],
                                    blocking=self.sqlbinlog['blocking'],
                                    log_file=self.sqlbinlog['log_file'],
                                    log_pos=self.sqlbinlog['log_pos'],
                                    only_schemas=self.sqlbinlog['only_schemas'],
                                    only_tables=self.sqlbinlog['only_tables'],
                                    only_events=self.sqlbinlog['only_events'],
                                    )
        for binlogevent in stream:
            binlogevent.dump()

        stream.close()
        # write_config('test', '1', '2')

    # elastic save
    def elastic_save(self, guid, doc):
        esclient = Elasticsearch([self.elastic])
        if not exists_by_doc_id(esclient,self.elastic, guid):
            esclient.create(
                index=self.elastic['index'],
                doc_type=self.elastic['type'],
                id=guid,
                body=doc,
            )
            self.logger.record('SYNC < %s-%s-%s > SUCCESS !' % (self.elastic['index'], self.elastic['type'], guid, ))
        else:
            self.logger.record('SYNC < %s-%s-%s > EXISTSED !' % (self.elastic['index'], self.elastic['type'], guid, ))



if __name__ == '__main__':
    sync = Sync()
    # sync.full_sql()
    sync.binlog()
