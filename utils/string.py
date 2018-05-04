import hashlib


def str_to_md5str(my_str):
    hash_md5 = hashlib.md5(my_str.encode('utf-8'))
    return hash_md5.hexdigest()
