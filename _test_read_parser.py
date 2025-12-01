import pytest
from pathlib import Path
import importlib.util
import sys

# 为避免导入整个 utils 包导致的慢启动，这里仅按文件路径加载 read.py
_READ_PATH = str(Path(__file__).resolve().parents[1] / 'utils' / 'read.py')
spec = importlib.util.spec_from_file_location('utils_read_module', _READ_PATH)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader, 'Cannot load utils/read.py'
spec.loader.exec_module(mod)
parse_pattern_sequence = mod.parse_pattern_sequence
extract_records = mod.extract_records


def test_block_end_and_extraction():
    text = "\n".join([
        "HEADER X",
        "A: 1",
        "B: 2.5",
        "C: 10",
        "C: 20",
        "TAIL Y",
        "B: 9.9",  # should be ignored by block end
    ])

    key_match = [
        r"\s*HEADER\s+(\w+)",
        [
            [r"\s*A:\s*(\d+)", r"\s*B:\s*(-?\d+\.\d+)"],
            [r"\s*C:\s*(\d+)"]
        ],
        r"\s*TAIL\s+(\w+)"
    ]

    data_type = [
        [str],
        [[ [int], [float] ], [[int]]],
        [str]
    ]

    labels = [
        "head",
        "block",
        "tail"
    ]

    result = parse_pattern_sequence(text, key_match=key_match, labels=labels, data_type=data_type)

    # 验证：有三类标签，且块中包含两条 C
    rounds = result.get("rounds", [])
    assert len(rounds) >= 1
    items = rounds[0].get("items", [])
    # 找到块项
    block_items = [it for it in items if it.get("level") == "block" and it.get("label") == "block"]
    assert block_items, "block item should exist"
    # 通过提取 API 选择 B 和第二条 C（重复）
    schema = [
        {"name": "B", "label": "B", "level": "leaf", "indices": [0], "cast": [float]},
        {"name": "C2", "label": "block", "level": "block", "mode": "repeat_flat", "repeat_index": 1}
    ]
    records = extract_records(result, schema)

    assert "B" in records and records["B"] == pytest.approx(2.5)
    assert "C2" in records and records["C2"] == 20


def test_leaf_label_extraction():
    text = "\n".join([
        "HEADER X",
        "A: 1",
        "B: 2.5",
        "C: 10",
        "C: 20",
        "TAIL Y",
    ])

    key_match = [
        r"\s*HEADER\s+(\w+)",
        [
            [r"\s*A:\s*(\d+)", r"\s*B:\s*(-?\d+\.\d+)"],
            [r"\s*C:\s*(\d+)"]
        ],
        r"\s*TAIL\s+(\w+)"
    ]

    data_type = [
        [str],
        [[ [int], [float] ], [[int]]],
        [str]
    ]

    # 扩展 labels：块内部叶子命名为 A、B、C
    labels = [
        "head",
        [["A","B"], ["C"]],
        "tail"
    ]

    result = parse_pattern_sequence(text, key_match=key_match, labels=labels, data_type=data_type)

    # 直接按叶子标签选择：第一个 B、第二个 C
    schema = [
        {"name": "B1", "label": "B", "level": "leaf", "indices": [0], "cast": [float]},
        {"name": "C2", "label": "C", "level": "leaf", "mode": "repeat_flat", "repeat_index": 1}
    ]
    print('ddfdfadsf')
    records = extract_records(result, schema)
    assert records["B1"] == pytest.approx(2.5)
    assert records["C2"] == 20

test_leaf_label_extraction()