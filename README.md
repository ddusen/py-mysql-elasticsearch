# py-mysql-elasticsearch

### 前言

最近有个项目需要引进全文检索和高级搜索功能，我在衡量了一些成熟的解决方案后，选择使用 ElasticSearch 来完成这项工作。
ES 的学习与使用还算简单，按照官方文档的说明，我大概花了一两天的工作之余的时间就能简单上手了 ES。

接下来要解决的就是下一个问题，如何把现有关联性数据库中数据同步到ES中。想必网上肯定有很多类似的轮子，经过一番查找，果
然有几个成熟的轮子(elasticsearch-jdbc, elasticsearch-river-mysql, go-mysql-elasticsearch)。 

- elasticsearch-jdbc：已经有两年没有更新了，我需要找一个比较旧的es版本，这样不知道未来会不会有坑，所以我就放弃了这
个轮子。
- elasticsearch-river-mysql：github上的start非常少，这让我有些担忧，所以我也放弃了这个轮子。
- go-mysql-elasticsearch：这个更新时间很近，而且start也有1000+，所以我选择了这个轮子。

通过一番研究，go-mysql-elasticsearch 确实很好用，但是由于我的需求有点奇怪，一个表有太多的关联关系，但我又需要放在
es的一个doc里面，导致 go-mysql-elasticsearch 没办法满足我的业务需求。经过一番思索，我决定自己写个轮子吧，称之为轮
子有些惭愧，毕竟自己的技术能力只是普通水平。

我的设计思路是：网上有很多轮子，这些轮子的终极目标应该是满足所有人的需求，但想要满足所有人的需求，那这个轮子必定很复
杂，同时使用者也会感到比较复杂。而且满足所有人，这应该属于一个“极限(Lim)问题”。**我设计的轮子，不能开箱即用，类似于半
陈品的东西，使用者只需编写少量的业务代码就可以实现自己需求。**为了达到这一点，我尽量减少复杂的代码，而且对关键代码做了
充分注释。

+-----------------+--------------+------+
| 文件            | 功能          | 说明 |
+-----------------+--------------+------+
| core/config.ini |   配置        |  MySQL、MYSQLBIN、ES 配置项  |
| core/sync.py   |  核心同步模块   | 封装了sql语句同步，sqlbin同步，es存储等功能 |
| core/process.py |  业务逻辑模块  |  处理业务逻辑，供核心同步模块调用，使用修改业务逻辑，一般就可以满足个人需求 |
| utils/\*       |  工具类        | 放了一些轮子会用到的工具类，比如：时间处理，字符串格式化等等 |
| requirements.txt | 项目依赖包 |  熟悉python的朋友都懂~ |
+-----------------+--------------+------+

目前轮子还是比较粗糙，不过使用起来完全没有问题，已经可以完全满足我的业务需求，包括全量数据同步和增量数据同步，所有的
操作都会记录到日志文件中，方便用户调错与分析。

如果需要引用代码，或直接拿过去用，不用告知，爽朗的用就行了。**开源与开放促进社会的发展与进步。**

### 需求

> 同步 MySQL 部分表数据到 ElasticSearch，处理 ManyToMany 的关系，数据存放到同一张表。

*****

### Mysql 表结构

> base_article

```
+-----------------+--------------+------+-----+---------+-------+
| Field           | Type         | Null | Key | Default | Extra |
+-----------------+--------------+------+-----+---------+-------+
| guid            | varchar(32)  | NO   | PRI | NULL    |       |
| title           | varchar(255) | NO   |     | NULL    |       |
| url             | varchar(200) | NO   |     | NULL    |       |
| pubtime         | datetime(6)  | NO   |     | NULL    |       |
| source          | varchar(80)  | NO   |     | NULL    |       |
| score           | int(11)      | NO   |     | NULL    |       |
| risk_keyword    | varchar(255) | NO   |     | NULL    |       |
| invalid_keyword | varchar(255) | NO   |     | NULL    |       |
| status          | int(11)      | NO   |     | NULL    |       |
+-----------------+--------------+------+-----+---------+-------+
```

> base_area

```
+-----------+-------------+------+-----+---------+----------------+
| Field     | Type        | Null | Key | Default | Extra          |
+-----------+-------------+------+-----+---------+----------------+
| id        | int(11)     | NO   | PRI | NULL    | auto_increment |
| name      | varchar(50) | NO   |     | NULL    |                |
| level     | int(11)     | NO   |     | NULL    |                |
| parent_id | int(11)     | YES  | MUL | NULL    |                |
+-----------+-------------+------+-----+---------+----------------+
```

> base_articlearea

```
+------------+-------------+------+-----+---------+----------------+
| Field      | Type        | Null | Key | Default | Extra          |
+------------+-------------+------+-----+---------+----------------+
| id         | int(11)     | NO   | PRI | NULL    | auto_increment |
| article_id | varchar(32) | NO   |     | NULL    |                |
| area_id    | int(11)     | NO   |     | NULL    |                |
+------------+-------------+------+-----+---------+----------------+
```

> base_category

```
+-----------+-------------+------+-----+---------+-------+
| Field     | Type        | Null | Key | Default | Extra |
+-----------+-------------+------+-----+---------+-------+
| id        | varchar(5)  | NO   | PRI | NULL    |       |
| name      | varchar(10) | NO   |     | NULL    |       |
| level     | int(11)     | NO   |     | NULL    |       |
| parent_id | varchar(5)  | YES  | MUL | NULL    |       |
+-----------+-------------+------+-----+---------+-------+
```

> base_articlecategory

```
+-------------+-------------+------+-----+---------+----------------+
| Field       | Type        | Null | Key | Default | Extra          |
+-------------+-------------+------+-----+---------+----------------+
| id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| article_id  | varchar(32) | NO   |     | NULL    |                |
| category_id | varchar(5)  | NO   |     | NULL    |                |
+-------------+-------------+------+-----+---------+----------------+
```

### ElasticSearch 文档结构

```
PUT /observer/article/528eb39855e876852c1f6371a82ea634
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
```

*****

### 功能说明

1. 基于 sql 语句的全量同步
2. 基于 mysqlbinlog 实时同步与更新
3. 日志记录
4. ...

*****

### Run

```
cd py-mysql-elasticsearch/

python3 -m venv VENV
source VENV/bin/activate
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --upgrade pip
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

python core/sync.py
```

END!