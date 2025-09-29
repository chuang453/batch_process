import pprint

# 示例数据
data = {
    'name': 'Alice',
    'age': 30,
    'hobbies': ['reading', 'coding', 'hiking'],
    'address': {
        'city': 'Beijing',
        'zipcode': '100001',
        'coordinates': [39.9042, 116.4074]
    },
    'friends': [
        {'name': 'Bob', 'age': 28},
        {'name': 'Charlie', 'age': 32}
    ]
}

# 使用 pprint 打印
ss1 = pprint.pformat(data indent = 2, width = 40)
pprint.pprint(ss1)
ss1 += 
print()
pprint.pprint(data, indent=2, width=40, depth=None, compact=True)