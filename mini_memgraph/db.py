from typing import List, Dict, Union, Set, Tuple


class Database:
    def __init__(self, address: str, port: int = None, user: str = None, password: str = None):
        self._address = address
        try:
            self._port = int(port)
        except ValueError:
            self._port = None

        self._user = user
        self._password = password

    def read(self, query: str, **kwargs):
        pass

    def write(self, query: str, **kwargs):
        pass

    def set_index(self, label: str, property_name: str = None):
        pass

    def write_nodes(self, node_list, label: str, id_val: str, id_val_label: str = None,
                    add_attributes: Union[List[str], bool] = True, update: bool = False, chunk_size=100000,
                    custom_query=None):
        pass

    def write_edges(self, edge_list: List[Dict], source_label: str, edge_label: str, target_label: str,
                    source_id_label: str = 'id', target_id_label: str = 'id',
                    add_attributes: Union[List, Set, Tuple] = None, chunk_size=100000, custom_query=None,
                    on_duplicate_edges='update'):
        pass

    def update_labels(self, id_list: List[Dict], match_labels: Union[str, List[str]],
                      new_labels: Union[str, List[str]]):
        pass

    def remove_node_label(self, remove_label: str, ids: List[int] = None):
        pass

    def set_node_attr(self, id_list: List[Dict], node_label: str, id_property: str, attr_name: str):
        pass

    def remove_node_attr(self, label: str, attr: str, chunk_size=10000):
        pass

    def label_exists(self, label: str) -> bool:
        pass

    def attr_exists(self, label: str, attr: str, edge: bool = False, search_limit: int = None) -> bool:
        pass

    def wipe_relationships(self, rel_label: str):
        query = f"MATCH ()-[r:{rel_label}]->() DELETE r"
        self.write(query)

    def wipe_duplicate_relationships(self, rel_label, source_node_label=None, attr=None, batch_size: int = 10000):
        pass

    def node_count(self, label: str, where: str = None) -> int:
        pass

    def set_degree(self, node_label: str, rel_label: str = None, target_label=None, where: str = None,
                   set_property: str = None, orientation: str = 'undirected') -> int:
        pass

    def attr_minimum(self, label: str, attr: str, where: str = None) -> int:
        pass

    def attr_maximum(self, label: str, attr: str, where: str = None) -> int:
        pass
