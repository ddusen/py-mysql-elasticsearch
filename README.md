# py-mysql-elasticsearch

### 需求

> 同步 MySQL 部分表数据到 ElasticSearch，处理 ManyToMany 的关系，数据存放到同一张表。

*****

### Mysql 表结构

> base_article

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

> base_area

+-----------+-------------+------+-----+---------+----------------+
| Field     | Type        | Null | Key | Default | Extra          |
+-----------+-------------+------+-----+---------+----------------+
| id        | int(11)     | NO   | PRI | NULL    | auto_increment |
| name      | varchar(50) | NO   |     | NULL    |                |
| level     | int(11)     | NO   |     | NULL    |                |
| parent_id | int(11)     | YES  | MUL | NULL    |                |
+-----------+-------------+------+-----+---------+----------------+

> base_articlearea

+------------+-------------+------+-----+---------+----------------+
| Field      | Type        | Null | Key | Default | Extra          |
+------------+-------------+------+-----+---------+----------------+
| id         | int(11)     | NO   | PRI | NULL    | auto_increment |
| article_id | varchar(32) | NO   |     | NULL    |                |
| area_id    | int(11)     | NO   |     | NULL    |                |
+------------+-------------+------+-----+---------+----------------+

> base_category

+-----------+-------------+------+-----+---------+-------+
| Field     | Type        | Null | Key | Default | Extra |
+-----------+-------------+------+-----+---------+-------+
| id        | varchar(5)  | NO   | PRI | NULL    |       |
| name      | varchar(10) | NO   |     | NULL    |       |
| level     | int(11)     | NO   |     | NULL    |       |
| parent_id | varchar(5)  | YES  | MUL | NULL    |       |
+-----------+-------------+------+-----+---------+-------+

> base_articlecategory

+-------------+-------------+------+-----+---------+----------------+
| Field       | Type        | Null | Key | Default | Extra          |
+-------------+-------------+------+-----+---------+----------------+
| id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| article_id  | varchar(32) | NO   |     | NULL    |                |
| category_id | varchar(5)  | NO   |     | NULL    |                |
+-------------+-------------+------+-----+---------+----------------+

### ElasticSearch 文档结构

PUT /observer/article/528eb39855e876852c1f6371a82ea634
{
    "source": "中国新闻网",
    "title": "对儿童安全构成威胁 指尖陀螺被欧盟列为危险品",
    "pubtime": "2018-03-14 00:00:00",
    "url": "http://dw.chinanews.com/chinanews/content.jsp?id=8467281&classify=zw&pageSize=6&language=chs",
    "guid": "528eb39855e876852c1f6371a82ea634",
    "score": 2,
    "category": [
        {
            "id": "0001",
            "text": "xxx"
        },
        {
            "id": "0002",
            "text": "xxx"
        }
    ],
    "area": [
        {
            "id": "269",
            "text": "咸宁"
        },
        {
            "id": "239",
            "text": "孝感"
        }
    ]
}

END!