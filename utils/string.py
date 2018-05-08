import hashlib

from datetime import date, datetime, timedelta


def str_to_md5str(my_str):
    hash_md5 = hashlib.md5(my_str.encode('utf-8'))
    return hash_md5.hexdigest()


def date_to_str(dt, pattern='%Y-%m-%d %H:%M:%S'):
	return dt.strftime(pattern)


def str_to_date(my_str, pattern='%Y-%m-%d %H:%M:%S'):
	return datetime.strptime(dt, pattern)
