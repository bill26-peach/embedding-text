from collections import OrderedDict


class LRUCache:
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: str):
        # 如果缓存中没有该条目，返回 None
        if key not in self.cache:
            return None
        else:
            # 移动到末尾，表示该条目是最近使用的
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value: str):
        # 如果缓存中已存在该条目，先将其移到末尾
        if key in self.cache:
            self.cache.move_to_end(key)

        # 添加或更新缓存
        self.cache[key] = value

        # 如果缓存超出了容量，移除最不常使用的条目
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
