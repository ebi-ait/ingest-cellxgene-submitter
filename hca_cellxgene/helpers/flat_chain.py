from typing import Optional


class FlatChain:
    # Behaves like a linked list but flattened.
    # Each value in the chain has a 'child_key' which is the key for another value in the dict
    # Constraint: only one link of each key value
    # Enables faster look ups than a normal linked list
    def __init__(self, root_key, root_value):
        self.__chain = {
            root_key: root_value
        }
        self.current = root_key, root_value
        self.__last_key = root_key

    def append(self, key, value) -> 'FlatChain':
        if key in self.__chain:
            raise KeyError("Key already exists in chain")

        value['parent_key'] = self.__last_key
        self.__chain[key] = value
        self.__chain[self.__last_key]['child_key'] = key

        self.set(key)
        self.__last_key = key

        return self

    def get_link(self, key: str) -> Optional[dict]:
        return self.__chain.get(key)

    def set(self, key: str):
        if key not in self.__chain:
            raise KeyError("Key not in chain")

        self.current = key, self.__chain[key]

    def next(self) -> 'FlatChain':
        try:
            next_key = self.current[1]['child_key']
            self.set(next_key)
        except KeyError:
            raise IndexError
        return self

    def prev(self) -> 'FlatChain':
        try:
            prev_key = self.current[1]['parent_key']
            self.set(prev_key)
        except KeyError:
            raise IndexError
        return self
