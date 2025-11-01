import hashlib
import json


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()



print(_sha256_hex('123'.encode('utf-8')))

test_dict = {
    'test1': 'test1',
    'test2': 'test2',
    'test3': 'test3'
}

test_json = json.dumps(test_dict)
print(_sha256_hex(test_json.encode('utf-8')))