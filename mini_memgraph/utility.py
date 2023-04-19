from typing import Dict, Union, List


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def filter_dict(d: Dict, keys: Union[List, str], exclude: bool = False) -> Dict:
    if type(keys) == str:
        keys = {keys}
    if exclude:
        return {k: v for k, v in d.items() if k not in keys}
    else:
        return {k: v for k, v in d.items() if k in keys}
