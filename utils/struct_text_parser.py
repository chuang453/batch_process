"""
logstruct: Declarative parsing of structured text into typed objects.

Features:
- Field(label, pattern, converter)
- Optional(Field(...))
- OneOf([Field1, Field2, ...])
- Repeat(label, items, until=...)
- Output as dataclass or Pydantic model
- Auto-generate JSON Schema

Author: Qwen (Alibaba Cloud)
"""

import re
from typing import (
    Any, List, Union, Callable, Optional as TypingOptional,
    Dict, Tuple, Type, get_type_hints, Literal
)
from dataclasses import dataclass, make_dataclass, fields
from enum import Enum


# ======================
# Schema Definition Nodes
# ======================

class ParseNode:
    """Base class for all parsing nodes."""
    pass


@dataclass
class Field(ParseNode):
    label: str
    pattern: str
    converter: TypingOptional[Union[Callable, List[Callable]]] = None
    # 新增：labels 作为每个分组的标签（优先级高于 group_labels）。
    labels: TypingOptional[List[str]] = None
    # 兼容：旧字段，用于多分组提供逐分组的标签名
    group_labels: TypingOptional[List[str]] = None


@dataclass
class Optional(ParseNode):
    node: Field  # Only support optional field (not Repeat/OneOf for simplicity)


@dataclass
class OneOf(ParseNode):
    choices: List[Field]


@dataclass
class Repeat(ParseNode):
    label: str
    items: List['ParseNode']
    until: str = r'[^\n]\Z'  # default: end of file


# ======================
# Core Parsing Engine
# ======================

def _apply_converter(groups: tuple, converter) -> List[Any]:
    if not groups:
        return []
    if converter is None:
        return list(groups)
    elif callable(converter):
        return [converter(g) for g in groups]
    elif isinstance(converter, list):
        conv_list = list(converter)
        if len(conv_list) < len(groups):
            filler = conv_list[-1] if conv_list else str
            conv_list += [filler] * (len(groups) - len(conv_list))
        return [
            conv_list[i](groups[i]) if conv_list[i] is not None else groups[i]
            for i in range(len(groups))
        ]
    else:
        return list(groups)


_PATTERN_CACHE: Dict[Tuple[str, bool], re.Pattern] = {}

def _compile_pattern(pattern: str, use_search: bool) -> re.Pattern:
    key = (pattern, use_search)
    pat = _PATTERN_CACHE.get(key)
    if pat is None:
        # 对于 search/match，我们统一编译原始正则；search/match 的差异通过调用函数体现
        try:
            pat = re.compile(pattern)
        except re.error:
            # 编译失败时回退为一个永不匹配的正则
            pat = re.compile(r'\A\Z')
        _PATTERN_CACHE[key] = pat
    return pat

def _match_line(line: str, pattern: str, use_search: bool = True):
    pat = _compile_pattern(pattern, use_search)
    return (pat.search(line) if use_search else pat.match(line))


def _effective_group_labels(field: Field, compiled: re.Pattern) -> TypingOptional[List[str]]:
    """Return the labels to use for each capture group of this field.
    Priority:
    1) field.labels if provided and matches capture count
    2) field.group_labels if provided and matches capture count
    3) named groups from regex pattern (order by group index)
    4) None (caller should treat as single label or list)
    """
    n = compiled.groups
    if n <= 0:
        return None
    if field.labels and len(field.labels) == n:
        return field.labels
    if field.group_labels and len(field.group_labels) == n:
        return field.group_labels
    # named groups
    if compiled.groupindex:
        # groupindex maps name->index; build ordered by index
        ordered = [None] * n
        for name, idx in compiled.groupindex.items():
            if 1 <= idx <= n:
                ordered[idx - 1] = name
        if all(x is not None for x in ordered):
            return ordered
    return None


def _infer_field_type(field: Field) -> Type:
    """Infer Python type for a field based on converter.

    规则：
    - 无转换器：无分组→str；1 个分组→str；多分组→List[str]
    - 单个转换器：若为内建类型（int/float/str/bool），返回该类型；否则返回 Any
    - 转换器列表：
        * 长度为 1 且为内建类型，返回该类型；
        * 否则返回 List[Any]
    """
    conv = field.converter
    group_count = field.pattern.count('(')
    def _is_builtin_type(fn):
        return fn in (int, float, str, bool)
    if conv is None:
        if group_count <= 1:
            return str
        return List[str]
    elif callable(conv):
        return conv if _is_builtin_type(conv) else Any
    elif isinstance(conv, list):
        if len(conv) == 1 and callable(conv[0]) and _is_builtin_type(conv[0]):
            return conv[0]
        return List[Any]
    else:
        return Any


def _parse_block(
    lines: List[str],
    schema: List[ParseNode],
    key_end: str,
    use_search: bool,
    start_idx: int
) -> Tuple[Dict[str, Any], int]:
    """
    Parse one block according to schema.
    Returns (record_dict, next_line_index)
    """
    record: Dict[str, Any] = {}
    idx = start_idx

    for node in schema:
        if idx >= len(lines):
            if not isinstance(node, Optional):
                raise ValueError(f"Unexpected end of input at {node}")
            continue

        line = lines[idx]

        # Check global termination
        if _match_line(line, key_end, use_search):
            return record, idx

        if isinstance(node, Field):
            matched = False
            temp_idx = idx
            while temp_idx < len(lines):
                l = lines[temp_idx]
                if _match_line(l, key_end, use_search):
                    return record, idx  # terminate early
                m = _match_line(l, node.pattern, use_search)
                if m:
                    groups = m.groups()
                    converted = _apply_converter(groups, node.converter)
                    # 计算有效分组标签
                    comp = _compile_pattern(node.pattern, use_search)
                    eff_labels = _effective_group_labels(node, comp)
                    if eff_labels:
                        for gi, glabel in enumerate(eff_labels):
                            if gi < len(converted):
                                record[glabel] = converted[gi]
                    else:
                        record[node.label] = converted[0] if len(converted) == 1 else converted
                    idx = temp_idx + 1
                    matched = True
                    break
                temp_idx += 1
            if not matched:
                if not isinstance(node, Optional):
                    raise ValueError(f"Required field {node.label} not found")
                # else: skip optional

        elif isinstance(node, Optional):
            # Try to match inner field
            inner = node.node
            temp_idx = idx
            matched = False
            while temp_idx < len(lines):
                l = lines[temp_idx]
                if _match_line(l, key_end, use_search):
                    break
                m = _match_line(l, inner.pattern, use_search)
                if m:
                    groups = m.groups()
                    converted = _apply_converter(groups, inner.converter)
                    comp = _compile_pattern(inner.pattern, use_search)
                    eff = _effective_group_labels(inner, comp)
                    if eff:
                        for gi, gl in enumerate(eff):
                            if gi < len(converted):
                                record[gl] = converted[gi]
                    else:
                        record[inner.label] = converted[0] if len(converted) == 1 else converted
                    idx = temp_idx + 1
                    matched = True
                    break
                temp_idx += 1
            if not matched:
                # Optional: just skip
                pass

        elif isinstance(node, OneOf):
            matched_choice = False
            for choice in node.choices:
                temp_idx = idx
                while temp_idx < len(lines):
                    l = lines[temp_idx]
                    if _match_line(l, key_end, use_search):
                        break
                    m = _match_line(l, choice.pattern, use_search)
                    if m:
                        groups = m.groups()
                        converted = _apply_converter(groups, choice.converter)
                        comp = _compile_pattern(choice.pattern, use_search)
                        eff = _effective_group_labels(choice, comp)
                        if eff:
                            for gi, gl in enumerate(eff):
                                if gi < len(converted):
                                    record[gl] = converted[gi]
                        else:
                            record[choice.label] = converted[0] if len(converted) == 1 else converted
                        idx = temp_idx + 1
                        matched_choice = True
                        break
                    temp_idx += 1
                if matched_choice:
                    break
            if not matched_choice:
                raise ValueError("None of OneOf choices matched")

        elif isinstance(node, Repeat):
            sub_records = []
            end_pat = node.until
            temp_idx = idx

            while temp_idx < len(lines):
                l = lines[temp_idx]
                if _match_line(l, end_pat, use_search) or _match_line(l, key_end, use_search):
                    break

                try:
                    sub_rec, new_idx = _parse_block(lines, node.items, end_pat, use_search, temp_idx)
                    if sub_rec or new_idx > temp_idx:
                        sub_records.append(sub_rec)
                        temp_idx = new_idx
                    else:
                        temp_idx += 1
                except ValueError:
                    temp_idx += 1

            record[node.label] = sub_records
            idx = temp_idx

    return record, idx


def parse_text(
    text: Union[str, List[str]],
    schema: List[ParseNode],
    key_end: str = r'[^\n]\Z',
    nmatchmax: int = float('inf'),
    use_search: bool = True
) -> List[Dict[str, Any]]:
    """
    Parse text using declarative schema.

    Args:
        text: Input text (string or list of lines)
        schema: List of Field/Repeat/Optional/OneOf
        key_end: Global termination pattern
        nmatchmax: Max number of top-level matches
        use_search: Use re.search (True) or re.match (False)

    Returns:
        List of parsed records as dictionaries.
    """
    if isinstance(text, str):
        lines = text.splitlines()
    else:
        lines = text

    results = []
    idx = 0
    count = 0

    while count < nmatchmax and idx < len(lines):
        try:
            rec, new_idx = _parse_block(lines, schema, key_end, use_search, idx)
            if new_idx == idx:
                # No progress — avoid infinite loop
                idx += 1
                continue
            if rec or new_idx > idx:
                results.append(rec)
                count += 1
            idx = new_idx
        except ValueError:
            idx += 1

    return results


# ======================
# Dataclass & Pydantic Output
# ======================

def _schema_to_dataclass_fields(schema: List[ParseNode]) -> List[Tuple[str, Type, Any]]:
    """Convert schema to dataclass field definitions."""
    fields_def = []

    def _get_type(node: ParseNode) -> Type:
        if isinstance(node, Field):
            t = _infer_field_type(node)
            return t
        elif isinstance(node, Optional):
            inner_t = _infer_field_type(node.node)
            return TypingOptional[inner_t]
        elif isinstance(node, OneOf):
            # Union of possible types (simplified to Any)
            return Any
        elif isinstance(node, Repeat):
            # Recursively build inner dataclass
            inner_cls = _schema_to_dataclass(node.items, f"Inner_{node.label}")
            return List[inner_cls]
        else:
            return Any

    for node in schema:
        if isinstance(node, (Field, Optional)):
            if isinstance(node, Field):
                comp = _compile_pattern(node.pattern, True)
                eff = _effective_group_labels(node, comp)
                if eff:
                    elem_type = _infer_field_type(Field(label=node.label, pattern=node.pattern, converter=(node.converter if isinstance(node.converter, list) and len(node.converter)==1 else (node.converter if callable(node.converter) else None))))
                    et = elem_type if elem_type in (int, float, str, bool) else Any
                    for glabel in eff:
                        fields_def.append((glabel, et, ...))
                else:
                    label = node.label
                    typ = _get_type(node)
                    fields_def.append((label, typ, ...))
            else:
                label = node.node.label
                typ = _get_type(node)
                fields_def.append((label, typ, ...))
        elif isinstance(node, OneOf):
            # Use first choice label as field name (or generate?)
            label = "one_of"
            typ = Any
            fields_def.append((label, typ, ...))
        elif isinstance(node, Repeat):
            typ = _get_type(node)
            fields_def.append((node.label, typ, ...))

    return fields_def


def _schema_to_dataclass(schema: List[ParseNode], cls_name: str = "ParsedRecord") -> Type:
    inner_fields = _schema_to_dataclass_fields(schema)
    return make_dataclass(cls_name, inner_fields)


def parse_to_dataclass(
    text: Union[str, List[str]],
    schema: List[ParseNode],
    cls_name: str = "Record",
    **kwargs
) -> List[Any]:
    """Parse and return list of dataclass instances."""
    dicts = parse_text(text, schema, **kwargs)
    RecordClass = _schema_to_dataclass(schema, cls_name)
    return [RecordClass(**d) for d in dicts]


def parse_to_pydantic(
    text: Union[str, List[str]],
    schema: List[ParseNode],
    cls_name: str = "Record",
    **kwargs
) -> List[Any]:
    """
    Parse and return list of Pydantic BaseModel instances.
    Requires 'pydantic' installed.
    """
    try:
        from pydantic import BaseModel, create_model
    except ImportError:
        raise ImportError("Pydantic not installed. Run: pip install pydantic")

    dicts = parse_text(text, schema, **kwargs)

    # Build field definitions for Pydantic
    fields_def = {}
    for node in schema:
        if isinstance(node, (Field, Optional)):
            if isinstance(node, Field):
                comp = _compile_pattern(node.pattern, True)
                eff = _effective_group_labels(node, comp)
                if eff:
                    elem_type = _infer_field_type(Field(label=node.label, pattern=node.pattern, converter=(node.converter if isinstance(node.converter, list) and len(node.converter)==1 else (node.converter if callable(node.converter) else None))))
                    et = elem_type if elem_type in (int, float, str, bool) else Any
                    for glabel in eff:
                        fields_def[glabel] = (et, ...)
                else:
                    label = node.label
                    typ = _infer_field_type(node)
                    fields_def[label] = (typ, ...)
            else:
                label = node.node.label
                typ = _infer_field_type(node.node)
                typ = TypingOptional[typ]
                fields_def[label] = (typ, ...)
        elif isinstance(node, OneOf):
            fields_def["one_of"] = (Any, ...)
        elif isinstance(node, Repeat):
            InnerModel = _schema_to_pydantic_inner(node.items, f"Inner_{node.label}")
            fields_def[node.label] = (List[InnerModel], ...)

    RecordModel = create_model(cls_name, **fields_def)
    return [RecordModel(**d) for d in dicts]


def _schema_to_pydantic_inner(items: List[ParseNode], cls_name: str) -> Type:
    try:
        from pydantic import BaseModel, create_model
    except ImportError:
        return dict  # fallback

    fields_def = {}
    for node in items:
        if isinstance(node, (Field, Optional)):
            if isinstance(node, Field):
                comp = _compile_pattern(node.pattern, True)
                eff = _effective_group_labels(node, comp)
                if eff:
                    elem_type = _infer_field_type(Field(label=node.label, pattern=node.pattern, converter=(node.converter if isinstance(node.converter, list) and len(node.converter)==1 else (node.converter if callable(node.converter) else None))))
                    et = elem_type if elem_type in (int, float, str, bool) else Any
                    for glabel in eff:
                        fields_def[glabel] = (et, ...)
                else:
                    label = node.label
                    typ = _infer_field_type(node)
                    fields_def[label] = (typ, ...)
            else:
                label = node.node.label
                typ = _infer_field_type(node.node)
                typ = TypingOptional[typ]
                fields_def[label] = (typ, ...)
        elif isinstance(node, OneOf):
            fields_def["one_of"] = (Any, ...)
        elif isinstance(node, Repeat):
            Inner = _schema_to_pydantic_inner(node.items, f"{cls_name}_{node.label}")
            fields_def[node.label] = (List[Inner], ...)
    return create_model(cls_name, **fields_def)


# ======================
# JSON Schema Generation
# ======================

def _type_to_json_schema(typ) -> Dict[str, Any]:
    """Convert Python type to JSON Schema fragment."""
    if typ == str:
        return {"type": "string"}
    elif typ == int:
        return {"type": "integer"}
    elif typ == float:
        return {"type": "number"}
    elif typ == bool:
        return {"type": "boolean"}
    elif hasattr(typ, "__origin__"):
        origin = typ.__origin__
        args = typ.__args__
        if origin is TypingOptional:
            return {**_type_to_json_schema(args[0]), "nullable": True}
        elif origin is list or origin is List:
            item_schema = _type_to_json_schema(args[0]) if args else {"type": "string"}
            return {"type": "array", "items": item_schema}
        elif origin is Union:
            return {"anyOf": [_type_to_json_schema(t) for t in args]}
    return {"type": "object"}  # fallback


def generate_json_schema(schema: List[ParseNode], title: str = "LogStruct Schema") -> Dict[str, Any]:
    """Generate JSON Schema (Draft 7) for the given parsing schema."""
    properties = {}

    def _add_node_to_props(node: ParseNode, props: dict):
        if isinstance(node, (Field, Optional)):
            if isinstance(node, Field):
                comp = _compile_pattern(node.pattern, True)
                eff = _effective_group_labels(node, comp)
                if eff:
                    elem_type = _infer_field_type(Field(label=node.label, pattern=node.pattern, converter=(node.converter if isinstance(node.converter, list) and len(node.converter)==1 else (node.converter if callable(node.converter) else None))))
                    et = elem_type if elem_type in (int, float, str, bool) else Any
                    for glabel in eff:
                        props[glabel] = _type_to_json_schema(et)
                else:
                    label = node.label
                    typ = _infer_field_type(node)
                    props[label] = _type_to_json_schema(typ)
            else:
                label = node.node.label
                typ = _infer_field_type(node.node)
                schema_frag = _type_to_json_schema(TypingOptional[typ])
                props[label] = schema_frag
        elif isinstance(node, OneOf):
            props["one_of"] = {"type": "object", "additionalProperties": True}
        elif isinstance(node, Repeat):
            inner_props = {}
            for item in node.items:
                _add_node_to_props(item, inner_props)
            props[node.label] = {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": inner_props,
                    "additionalProperties": False
                }
            }

    for node in schema:
        _add_node_to_props(node, properties)

    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": title,
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
        "required": [
            # Field: 若按分组标签展开，则不要求原 label；否则要求该 label
            *(
                [node.label] if isinstance(node, Field) and _effective_group_labels(node, _compile_pattern(node.pattern, True)) is None else []
            ),
            # Repeat: 要求该数组字段存在
            *([node.label] for node in schema if isinstance(node, Repeat)),
            # OneOf: 不强制特定字段为必填，交由 anyOf 表达（此处略简化）
        ]
    }

import pandas as pd
from typing import Any, List, Union, Dict
from collections.abc import Iterable

def flatten_to_dataframe(
    records: List[Any],
    explode_field: TypingOptional[str] = None,
    prefix: str = "",
    _parent_data: TypingOptional[Dict] = None
) -> pd.DataFrame:
    """
    将嵌套的解析结果展平为 pandas DataFrame。
    
    参数:
        records: 来自 parse_to_dataclass / parse_to_pydantic / parse_text 的结果列表
        explode_field: 要展开的字段名（如 "blades"）。若为 None，自动选择第一个 list 字段。否则展开所有
        prefix: （内部递归用）嵌套字段前缀，如 "stage_blade_"
        _parent_data: （内部用）上层非列表字段
    
    返回:
        pd.DataFrame，每行代表一个最内层实体（如一片桨叶）
    """
    if not records:
        return pd.DataFrame()

    # 辅助函数：安全获取属性或字典值
    def get_value(obj, key):
        if isinstance(obj, dict):
            return obj.get(key)
        else:
            return getattr(obj, key, None)

    # 辅助函数：判断是否为非字符串的可迭代对象（即列表/元组）
    def is_list_like(value):
        return (
            isinstance(value, Iterable) 
            and not isinstance(value, (str, bytes))
            and not hasattr(value, 'items')  # 排除 dict-like
        )

    rows = []
    sample = records[0]

    # 获取所有字段名
    if isinstance(sample, dict):
        all_keys = list(sample.keys())
    else:
        # dataclass 或 pydantic
        if hasattr(sample, '__dataclass_fields__'):
            all_keys = [f.name for f in fields(sample)]
        elif hasattr(sample, 'model_fields'):  # pydantic v2
            all_keys = list(sample.model_fields.keys())
        elif hasattr(sample, '__fields__'):   # pydantic v1
            all_keys = list(sample.__fields__.keys())
        else:
            all_keys = [k for k in dir(sample) if not k.startswith('_') and not callable(getattr(sample, k))]

    # 确定要 explode 的字段
    list_fields = [k for k in all_keys if is_list_like(get_value(sample, k))]
    if explode_field is None:
        explode_field = list_fields[0] if list_fields else None

    for record in records:
        # 提取非列表字段（标量字段）
        scalar_data = {}
        list_data = {}

        for key in all_keys:
            value = get_value(record, key)
            if key == explode_field and is_list_like(value):
                list_data[key] = value
            elif not is_list_like(value):
                scalar_data[key] = value
            # 忽略其他 list 字段（非 explode_field）

        # 合并父级数据（用于递归）
        current_scalar = {**( _parent_data or {} ), **scalar_data}

        if explode_field and explode_field in list_data:
            sub_records = list_data[explode_field]
            if not sub_records:
                # 空列表：仍保留一行（全 null 子字段）
                rows.append(current_scalar.copy())
            else:
                # 递归展平子记录
                sub_df = flatten_to_dataframe(
                    sub_records,
                    explode_field=None,  # 在子层自动选下一个 list
                    prefix=f"{explode_field}_",
                    _parent_data=current_scalar
                )
                rows.extend(sub_df.to_dict('records'))
        else:
            # 无列表字段：直接作为一行
            rows.append(current_scalar)

    # 统一列名（加上前缀）
    if prefix and rows:
        renamed_rows = []
        for row in rows:
            new_row = {}
            for k, v in row.items():
                new_row[f"{prefix}{k}" if not k.startswith(prefix) else k] = v
            renamed_rows.append(new_row)
        rows = renamed_rows

    return pd.DataFrame(rows)

# ======================
# Export Public API
# ======================

__all__ = [
    "Field",
    "Optional",
    "OneOf",
    "Repeat",
    "parse_text",
    "parse_to_dataclass",
    "parse_to_pydantic",
    "generate_json_schema",
    "flatten_to_dataframe"
]