from mini_memgraph.db import Database
from typing import List, Dict, Union, Set, Tuple
import mgclient

from mini_memgraph.utility import chunks, filter_dict


class Memgraph(Database):
    connection: mgclient.Connection
    connected: bool = False
    autocommit: bool = True

    def __init__(self, address: str = 'localhost', port: int = 7687, user: str = None, password: str = None):
        super().__init__(address, port, user, password)

    def _connect(self):
        try:
            self.connection = mgclient.connect(host=self._address,
                                               port=self._port,
                                               username='' if self._user is None else self._user,
                                               password='' if self._password is None else self._password)
            self.connected = True

        except mgclient.OperationalError as e:
            raise e

    def _disconnect(self):
        if self.connected:
            self.connection.close()
            self.connected = False

    def __repr__(self):
        return f"Memgraph(address='{self._address}', port={self._port}, user='{self._user}', password='{self._password}')"

    def __del__(self):
        self._disconnect()

    @staticmethod
    def _get_return_labels(query: str) -> Union[List[str], None]:
        try:
            return_statement = query.split('RETURN')[1]
        except IndexError:
            return None
        strip_keywords = {'ORDER BY', 'LIMIT'}
        for keyword in strip_keywords:
            if keyword in return_statement:
                return_statement = return_statement.split(keyword)[0]
        return_params = return_statement.split(',')
        cleaned_params = [x.split('AS')[-1].strip() for x in return_params]

        return cleaned_params

    @staticmethod
    def _unpack_results(results: List[Tuple]) -> List[Tuple]:
        return [tuple(item.properties
                      if (isinstance(item, mgclient.Node) or isinstance(item, mgclient.Relationship))
                      else item
                      for item in record) for record in results]

    @staticmethod
    def _label_results(results: List[Tuple], labels: List[str]) -> List[Dict]:
        return [{label: value
                 for label, value in zip(labels, record)}
                for record in results]

    def _execute(self, query: str, **kwargs):
        self.connection.autocommit = self.autocommit
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, kwargs)
        except mgclient.DatabaseError as e:
            print(query)
            print(kwargs)
            cursor.close()
            raise e
        if cursor.rowcount > 0:
            res = cursor.fetchall()
            labels = self._get_return_labels(query)
            if labels is not None:
                unpacked = self._unpack_results(res)
                output = self._label_results(unpacked, labels)
            else:
                output = res
        else:
            output = None
        cursor.close()
        return output

    def read(self, query: str, **kwargs):
        try:
            self._connect()
            result = self._execute(query, **kwargs)
            self.connection.commit()
            self._disconnect()
            return result
        finally:
            self._disconnect()

    def write(self, query: str, commit=True, **kwargs):
        try:
            self._connect()

            res = self._execute(query, **kwargs)

            self.connection.commit()
            self._disconnect()
            return res
        finally:
            self._disconnect()

    def set_index(self, label: str, property_name: str = None):
        if property_name is not None:
            query = f"CREATE INDEX ON :{label}({property_name});"
        else:
            query = f"CREATE INDEX ON :{label};"
        self.write(query)
        res = self.read('SHOW INDEX INFO')

        index_present = len([index for index in res if index[1] == label and index[2] == property_name]) > 0
        if not index_present:
            raise Exception(f'Index not added. Current indexes are {res}')

    def set_constraint(self, label:str, property:str):
        query = f"CREATE CONSTRAINT ON (n:{label}) ASSERT n.{property} IS UNIQUE"
        self.write(query)
        res = self.read('SHOW CONSTRAINT INFO')
        return res

    def write_nodes(self, node_list, label: str, id_val: str, id_val_label: str = None,
                    add_attributes: Union[List[str], bool] = True, update=False, chunk_size=100000, custom_query=None):

        if custom_query is None:
            if id_val_label is None:
                id_val_label = id_val

            keep_keys = [id_val_label]
            if add_attributes:
                if isinstance(add_attributes, list):
                    keep_keys += add_attributes
                node_list = [filter_dict(record, keep_keys) for record in node_list]

            query = f"UNWIND $node_list as row " \
                    f"MERGE (n:{label} {{{id_val}:row.{id_val_label}}}) " \
                    f"ON CREATE SET n += row"

            if update:
                update_statement = "ON MATCH SET n += row"
                query = ' '.join([query,update_statement])

        else:
            query = custom_query

        for node_chunk in chunks(node_list, chunk_size):
            self.write(query, node_list=node_chunk)

    def write_edges(self, edge_list: List[Dict], source_label: str, edge_label: str, target_label: str,
                    source_id_label: str = 'id', target_id_label: str = 'id',
                    add_attributes: Union[List, Set, Tuple] = None,
                    chunk_size=100000,
                    custom_query=None,
                    on_duplicate_edges: str = 'update'):

        if on_duplicate_edges not in ('update', 'increment'):
            raise ValueError(f'on_duplicate_edges must be either "update" or "increment". {on_duplicate_edges=}')
        if add_attributes is not None:
            attributes_statement = ", ".join([
                f"r.{val} = row.{val}" for val in add_attributes
            ])
            update_create_set = f"ON CREATE SET {attributes_statement}"
            increment_create_set = f"ON CREATE SET {attributes_statement}, r.weight = 1"
            update_match_set = f"ON MATCH SET {attributes_statement}"
        else:
            update_create_set = f""
            increment_create_set = f"ON CREATE SET r.weight = 1"
            update_match_set = ''

        on_duplicate_options = {
            'update': {
                'create': update_create_set,
                'match': update_match_set
            },
            'increment': {
                'create': increment_create_set,
                'match': 'ON MATCH SET r.weight = r.weight +1'}
        }

        unwind_statement = "UNWIND $edge_list AS row"
        match_nodes = f"MATCH (s:{source_label} {{id:row.source}}), (t:{target_label} {{id:row.target}})"
        merge_rel = f"MERGE (s)-[r:{edge_label}]->(t)"
        on_create = on_duplicate_options[on_duplicate_edges]['create']
        on_match = on_duplicate_options[on_duplicate_edges]['match']

        query = ' '.join([unwind_statement, match_nodes, merge_rel, on_create, on_match])
        for edge_chunk in chunks(edge_list, chunk_size):
            self.write(query, edge_list=edge_chunk)

    def update_labels(self, id_list: List[Dict], match_labels: Union[str, List[str]],
                      new_labels: Union[str, List[str]], chunk_size=100000):
        if isinstance(match_labels, list):
            match_labels = ':'.join(match_labels)
        if isinstance(new_labels, list):
            new_labels = ':'.join(new_labels)

        query = f"UNWIND $id_list AS row " \
                         f"MATCH (n:{match_labels.upper()}) " \
                         f"WHERE n.id = row.id " \
                         f"SET n:{new_labels.upper()}"
        for id_chunk in chunks(id_list, chunk_size):
            self.write(query, id_list=id_chunk)

    def set_node_attr(self, id_list: List[Dict], node_label: str, id_property: str, attr_name: str):
        query = f"UNWIND $import_data AS row " \
                         f"MATCH (n:{node_label}) " \
                         f"WHERE n.{id_property} = row.{id_property} " \
                         f"SET n.{attr_name} = row.{attr_name}"
        res = self.write(query=query, import_data=id_list)
        return res

    def remove_node_attr(self, label: str, attr: str, chunk_size=10000):
        query = f"MATCH (n:{label}) WHERE n.{attr} IS NOT NULL SET n.{attr} = NULL RETURN count(n)"
        res = self.write(query)
        return res

    def remove_node_label(self, remove_label: str, ids: List[int] = None):
        query = f"MATCH (n:{remove_label}) REMOVE n:{remove_label}"
        self.write(query)

    def label_exists(self, label: str) -> bool:
        return self.node_count(label) > 0

    def attr_exists(self, label: str, attr: str, edge: bool = False, search_limit: int = None) -> bool:
        if search_limit is not None:
            limit_str = " WITH r LIMIT $limit_num "
        else:
            limit_str = " "

        if edge:
            query = f"MATCH ()-[r:{label}]-(){limit_str}WHERE r.{attr} IS NOT NULL RETURN r LIMIT 1"
        else:
            query = f"MATCH (n:{label}){limit_str}WHERE n.{attr} IS NOT NULL RETURN n LIMIT 1"
        r = self.read(query, limit_num=search_limit)
        if r is None:
            return False
        else:
            return len(r) > 0

    def wipe_duplicate_relationships(self, rel_label, source_node_label=None, attr=None, batch_size: int = 10000):
        if source_node_label is not None:
            source_node = f"(a:{source_node_label} {{{attr}}})"
        else:
            source_node = "(a)"
        query = f"MATCH {source_node}-[r:{rel_label.upper()}]->(b) " \
                "WITH a, b, COLLECT(r) AS rr " \
                "WHERE SIZE(rr) > 1 " \
                f"WITH rr, count(rr) AS freq LIMIT {batch_size} " \
                "FOREACH (r IN TAIL(rr) | DELETE r) " \
                "RETURN freq"

        count_query = f"MATCH {source_node}-[r:{rel_label.upper()}]->(b) " \
                      f"RETURN count(r) AS remaining_rels"
        iter = 1

        res = self.read(query=count_query)
        last_count = res[0]['remaining_rels']
        while True:
            self.write(query=query)

            res = self.read(query=count_query)
            current_count = res[0]['remaining_rels']
            if current_count == last_count:
                break
            else:
                last_count = current_count
                iter += 1

    def node_count(self, label: str, where: str=None) -> int:
        if where is not None:
            if not where.startswith('WHERE '):
                where = 'WHERE ' + where
        else:
            where = ''
        match_query = f'MATCH (n:{label})'
        return_query = 'RETURN count(n) AS n_nodes'
        query = ' '.join([match_query,where,return_query])
        return self.read(query)[0]['n_nodes']

    def _attr_extreme(self, label:str, attr:str, where:str=None, _min=True) -> int:
        match_query = f"MATCH (n:{label})"

        if where is not None and not where.startswith('WHERE '):
            where = 'WHERE ' + where
        where = where if not None else""

        if _min:
            aggregation = "min"
        else:
            aggregation = "max"

        return_query = f"RETURN {aggregation}(n.{attr}) AS value"
        query = ' '.join([match_query, where, return_query])
        res = self.read(query)
        return res[0]['value']

    def attr_minimum(self, label:str, attr:str, where:str=None) -> int:
        return self._attr_extreme(label, attr, where, _min=True)

    def attr_maximum(self, label:str, attr:str, where:str=None) -> int:
        return self._attr_extreme(label, attr, where, _min=False)

    def set_degree(self, node_label:str, rel_label:str=None, target_label:str = None,
                   where:str=None, set_property:str=None, orientation: str='undirected') -> int:
        if rel_label is not None:
            rel_label = f'-[r:{rel_label}]-'
        else:
            rel_label = "-[r]-"
        if orientation.lower() == 'in':
            rel_label = '<' + rel_label
        elif orientation.lower() == 'out':
            rel_label = rel_label + '>'

        if where is not None:
            if not where.upper().startswith('WHERE '):
                where = 'WHERE '+ where
        else:
            where = ""

        if set_property is None:
            if orientation == 'undirected':
                set_property = 'degree'
            else:
                set_property = f"{orientation}_degree"
        if target_label is None:
            target_label = node_label
        match_query = f"MATCH (s:{node_label}){rel_label}(t:{target_label})"

        with_query = "WITH s, count(r) AS deg"
        set_query = f"SET s.{set_property} = deg"
        return_query = "RETURN count(s) AS total_nodes"
        query = ' '.join([match_query, where, with_query, set_query, return_query])

        res = self.write(query)
        return res[0]['total_nodes']
