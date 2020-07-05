from typing import Union, Sequence


def _trans_str_seq(val: Union[str, Sequence[str]]) -> Sequence[str]:
    return (val,) if isinstance(val, str) else val
