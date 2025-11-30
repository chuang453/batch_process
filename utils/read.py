from typing import Any, List, Union, Callable, Optional, Tuple
import re

def get_first_elem(a: Any) -> str:
    """递归获取嵌套列表/元组中的第一个字符串元素（用于确定终止模式）"""
    if isinstance(a, (list, tuple)):
        if not a:
            raise ValueError("Empty list/tuple in key_match is not allowed")
        return get_first_elem(a[0])
    elif isinstance(a, str):
        return a
    else:
        raise TypeError(f"Expected str/list/tuple, got {type(a)} in key_match")
    

# 辅助函数：从嵌套结构中提取最后一行字符串
def _get_last_line_from_nested(nested: Any) -> str:
    if isinstance(nested, str):
        return nested
    elif isinstance(nested, (list, tuple)):
        for item in reversed(nested):
            try:
                return _get_last_line_from_nested(item)
            except TypeError:
                continue
    raise TypeError("Could not extract last line from nested structure")


def _infer_data_type(key_match: List[Any]) -> List[Any]:
    dt = []
    for pat in key_match:
        if isinstance(pat, (list, tuple)):
            dt.append(_infer_data_type(pat))
        else:
            if '(' in pat and ')' in pat:
                dt.append(float)
            else:
                dt.append([])
    return dt


def robust_read_and_find(
    lines: List[str],
    key_match: List[Any],
    key_end: str = r'[^\n]\Z',  # 默认：文件末尾（实际由调用者控制）
    data_type: Optional[List[Any]] = None,
    nmatchmax: int = float('inf'),
    current_line_index: int = 0,
    use_search: bool = True,      # ← 新增参数！
    _depth: int = 0,
) -> Tuple[List[List[str]], List[List[Any]]]:
    """
    从文本行列表中按 key_match 模式提取结构化数据。

    参数:
        lines: 所有文本行（不含换行符），由外部一次性读入
        key_match: 匹配模式（字符串或嵌套列表）
        key_end: 终止正则（用于循环块）
        data_type: 与 key_match 结构一致的类型转换函数
        nmatchmax: 最大完整匹配轮数
        current_line_index: 当前处理到第几行（内部递归用）
        use_search (bool): 
            - True (default): 使用 re.search（在整个行中搜索）
            - False: 使用 re.match（仅匹配行首）
        _depth: 递归深度（仅用于调试）

    返回:
        (matched_lines_list, extracted_groups_list)
        - matched_lines_list[i]: 第 i 轮匹配到的所有原始行（嵌套结构）
        - extracted_groups_list[i]: 第 i 轮提取的数据（经 data_type 转换）
    """
    if data_type is None:
        data_type = _infer_data_type(key_match)

    if len(key_match) != len(data_type):
        raise ValueError(f"key_match and data_type must have same length. "
                         f"Got {len(key_match)} vs {len(data_type)}")

    matched_lines_all: List[List[Any]] = []
    extracted_groups_all: List[List[Any]] = []

    line_idx = current_line_index
    nkey = len(key_match)
    match_round = 0

    # 选择匹配函数
    re_func = re.search if use_search else re.match

    while match_round < nmatchmax and line_idx < len(lines):
        current_matched_lines: List[Any] = []
        current_extracted_groups: List[Any] = []
        k = 0

        while k < nkey and line_idx < len(lines):
            pattern = key_match[k]
            dtype = data_type[k]

            if dtype is None:
                k += 1
                continue

            if isinstance(pattern, (list, tuple)):
                # 嵌套块：递归处理
                end_pattern = get_first_elem(key_match[k + 1]) if k + 1 < nkey else key_end
                sub_lines, sub_groups = robust_read_and_find(
                    lines,
                    list(pattern),
                    key_end=end_pattern,
                    data_type=dtype,
                    nmatchmax=float('inf'),
                    current_line_index=line_idx,
                    use_search=use_search,   # ← 传递下去
                    _depth=_depth + 1
                )

                if not sub_lines:
                    break

                # 推进行索引（通过最后一行定位）
                last_line = _get_last_line_from_nested(sub_lines)
                while line_idx < len(lines) and lines[line_idx] != last_line:
                    line_idx += 1
                if line_idx < len(lines):
                    line_idx += 1

                current_matched_lines.append(sub_lines)
                current_extracted_groups.append(sub_groups)
                k += 1

            else:
                if not isinstance(pattern, str):
                    raise TypeError(f"Pattern must be str, list, or tuple; got {type(pattern)}")

                line = lines[line_idx]

                # 检查全局终止条件（也用相同匹配模式）
                if re_func(key_end, line):
                    return matched_lines_all, extracted_groups_all

                match = re_func(pattern, line)
                if match:
                    current_matched_lines.append(line)
                    groups = list(match.groups())

                    if dtype == []:
                        processed = groups
                    elif callable(dtype):
                        processed = dtype(groups)
                        if not isinstance(processed, list):
                            raise ValueError(f"data_type function must return list; got {type(processed)}")
                    elif isinstance(dtype, (list, tuple)):
                        if not groups:
                            processed = []
                        else:
                            dtype_list = list(dtype)
                            if len(dtype_list) < len(groups):
                                filler = dtype_list[-1] if dtype_list else str
                                dtype_list += [filler] * (len(groups) - len(dtype_list))
                            processed = [
                                dtype_list[i](groups[i]) if dtype_list[i] is not None else groups[i]
                                for i in range(len(groups))
                            ]
                    else:
                        raise TypeError(f"Invalid data_type entry: {dtype}")

                    current_extracted_groups.extend(processed)
                    k += 1
                    line_idx += 1
                else:
                    line_idx += 1

        if k == nkey:
            matched_lines_all.append(current_matched_lines)
            extracted_groups_all.append(current_extracted_groups)
            match_round += 1
        else:
            break

    return matched_lines_all, extracted_groups_all

def read_and_find_from_file(
    file_path: str,
    key_match: List[Any],
    key_end: str = r'[^\n]\Z',
    data_type: Optional[List[Any]] = None,
    nmatchmax: int = float('inf'),
    encoding: str = 'utf-8'
):
    with open(file_path, 'r', encoding=encoding) as f:
        lines = [line.rstrip('\r\n') for line in f]
    return robust_read_and_find(lines, key_match, key_end, data_type, nmatchmax)
