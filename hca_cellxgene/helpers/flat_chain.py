from typing import Optional


class FlatChain:
    # Behaves like a linked list but flattened.
    # Each value in the chain has a 'child_key' which is the key for another value in the dict
    # Constraint: only one link of each key value
    # Enables faster look ups than a normal linked list
    def __init__(self, root_key, root_value):
        self.chain = {
            root_key: root_value
        }
        self.current = root_value

    def append(self, key, value) -> 'FlatChain':
        self.chain[key] = value
        self.current['child_key'] = key
        self.current = value
        return self

    def get_link(self, key: str) -> Optional[dict]:
        return self.chain.get(key)
