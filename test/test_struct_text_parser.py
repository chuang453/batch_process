import pytest
from pathlib import Path
import importlib.util

# 直接按文件路径加载 utils/struct_text_parser.py，避免与 site-packages 中的同名包冲突
_STP_PATH = str(Path(__file__).resolve().parents[1] / 'utils' / 'struct_text_parser.py')
spec = importlib.util.spec_from_file_location('struct_text_module', _STP_PATH)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader, 'Cannot load utils/struct_text_parser.py'
spec.loader.exec_module(mod)

Field = mod.Field
OptField = mod.Optional
OneOf = mod.OneOf
Repeat = mod.Repeat
parse_text = mod.parse_text


def test_repeat_optional_oneof_parse():
    text = "\n".join([
        "HEADER A",
        "X: 10",
        "Y: 3.14",
        "TYPE: ALPHA",
        "ITEM: v1",
        "ITEM: v2",
        "FOOTER",
        "HEADER B",
        "X: 20",
        "TYPE: BETA",
        "ITEM: v9",
        "FOOTER",
    ])

    schema = [
        Field(label="header", pattern=r"^HEADER\s+(\w+)", converter=str),
        OptField(Field(label="x", pattern=r"^X:\s*(\d+)", converter=int)),
        OptField(Field(label="y", pattern=r"^Y:\s*(-?\d+\.\d+)", converter=float)),
        OneOf([
            Field(label="type_alpha", pattern=r"^TYPE:\s*ALPHA"),
            Field(label="type_beta", pattern=r"^TYPE:\s*BETA"),
        ]),
        Repeat(label="items", items=[
            Field(label="item", pattern=r"^ITEM:\s*(\w+)", converter=str)
        ], until=r"^FOOTER")
    ]

    records = parse_text(text, schema, key_end=r"^END$", use_search=False)

    assert len(records) == 2
    # First block
    r0 = records[0]
    assert r0["header"] == "A"
    assert r0.get("x") == 10
    assert r0.get("y") == pytest.approx(3.14)
    assert "type_alpha" in r0
    assert r0.get("items") == [{"item": "v1"}, {"item": "v2"}]

    # Second block
    r1 = records[1]
    assert r1["header"] == "B"
    assert r1.get("x") == 20
    assert "y" not in r1  # optional missing
    assert "type_beta" in r1
    assert r1.get("items") == [{"item": "v9"}]
    print(records)



def test_field_group_labels_expand():
    # 示例：单行包含三个分组，使用 group_labels 将分组拆成独立键
    text = "\n".join([
        "V: 1.0 2.5 -3.2",
    ])

    schema = [
        Field(label="vec", pattern=r"^V:\s*(-?\d+\.\d+)\s+(-?\d+\.\d+)\s+(-?\d+\.\d+)", converter=float, group_labels=["vx","vy","vz"])
    ]

    records = parse_text(text, schema, key_end=r"^END$", use_search=False)
    assert len(records) == 1
    r = records[0]
    # 期望拆分为独立键，而不是 vec: [..]
    assert r["vx"] == pytest.approx(1.0)
    assert r["vy"] == pytest.approx(2.5)
    assert r["vz"] == pytest.approx(-3.2)

test_repeat_optional_oneof_parse()  
test_field_group_labels_expand()  


def test_parse_output_blocks_example():
    # 使用真实文件 utils/58-ph-left-VD-M3.out，提取 OUTPUT = 区域作为数据块
    out_path = Path(__file__).resolve().parents[1] / 'utils' / '58-ph-left-VD-M3.out'
    assert out_path.exists(), f"Missing test file: {out_path}"

    text = out_path.read_text(encoding='utf-8', errors='ignore')

    # 记录在遇到下一条 OUTPUT = 前结束；首字段匹配 OUTPUT = 标题行
    schema = [
        Field(label="output", pattern=r"^\s*OUTPUT\s*=\s*(.+)$", converter=str),
        # 收集该块中的所有正文行，直到下一个 OUTPUT =
        Repeat(label="body", items=[
            Field(label="line", pattern=r"^(.+)$", converter=str)
        ], until=r"^\s*OUTPUT\s*=")
    ]

    records = parse_text(text, schema, key_end=r"^\s*OUTPUT\s*=", use_search=False, nmatchmax=5)

    # 应至少解析到一个 OUTPUT 区块
    assert len(records) >= 1
    r0 = records[0]
    assert isinstance(r0.get("output"), str)
    # 验证首个块的标题与正文包含关键行片段
    # 示例文件在约 5812 行有：OUTPUT = AIRFRAME SENSOR  1 ROTOR 1
    assert "AIRFRAME SENSOR" in r0["output"]
    body_lines = [b.get("line") for b in r0.get("body", [])]
    # 正文应至少包含 OUTPUT KIND 或 COMPONENT 信息行
    assert any("OUTPUT KIND" in ln for ln in body_lines)
    assert any("COMPONENT" in ln for ln in body_lines)

test_parse_output_blocks_example()


def test_parse_output_blocks_to_dataframe():
    # 复杂提取：解析 OUTPUT 标题、关键元数据与正文行，并展平为 DataFrame
    out_path = Path(__file__).resolve().parents[1] / 'utils' / '58-ph-left-VD-M3.out'
    assert out_path.exists(), f"Missing test file: {out_path}"

    text = out_path.read_text(encoding='utf-8', errors='ignore')

    # Schema:
    # - OUTPUT 标题行
    # - 可选的元数据：OUTPUT KIND、RESPONSE KIND、COMPONENT
    # - 正文为重复的任意行，直到遇到下一条 OUTPUT =
    schema = [
        Field(label="output", pattern=r"^\s*OUTPUT\s*=\s*(.+)$", converter=str),
        OptField(Field(label="output_kind", pattern=r"^\s*OUTPUT\s+KIND\s*=\s*(.+)$", converter=str)),
        OptField(Field(label="response_kind", pattern=r"^\s*RESPONSE\s+KIND\s*=\s*(.+)$", converter=str)),
        OptField(Field(label="component", pattern=r"^\s*COMPONENT\s*=\s*(.+)$", converter=str)),
        Repeat(label="body", items=[
            # 先尝试解析 "KEY = VALUE" 形式
            OneOf([
                Field(label="kv", pattern=r"^\s*([A-Z][A-Z0-9 _-]+)\s*=\s*(.+)$", converter=str),
                # 其次，保留整行作为文本
                Field(label="line", pattern=r"^(.+)$", converter=str)
            ])
        ], until=r"^\s*OUTPUT\s*=")
    ]

    records = parse_text(text, schema, key_end=r"^\s*OUTPUT\s*=", use_search=False, nmatchmax=3)
    assert len(records) >= 1

    # 展平到 DataFrame：按 body 展开，每行一条正文项
    df = mod.flatten_to_dataframe(records, explode_field="body")

    # 至少应有一些行与必要的列
    assert not df.empty
    for col in ["output", "output_kind", "response_kind", "component"]:
        assert col in df.columns

    # 验证包含示例中的关键词（AIRFRAME SENSOR、COMPONENT 等）
    assert df['output'].astype(str).str.contains("AIRFRAME SENSOR").any()
    assert df['component'].astype(str).str.contains("AIRFRAME").any()

    # 由于 body 展开，body_line 或 kv 字段可能存在，确保至少存在其一
    has_line = any(c.startswith('body_') for c in df.columns)
    assert has_line

test_parse_output_blocks_to_dataframe()


def test_labels_per_group_and_named_groups():
    # 演示：命名分组自动作为键；labels 明确指定每个分组变量名
    text = "\n".join([
        "OUTPUT = HEADER ONE",
        "COMPONENT = AIRFRAME",
        "V: 1.0 2.5 -3.2",
        "END",
        "OUTPUT = HEADER TWO",
        "COMPONENT = ROTOR",
        "V: -1 0 3.5",
        "END",
    ])

    schema = [
        # 未显式 labels，但使用命名组 (?P<header>...)，应生成键 'header'
        Field(label="out", pattern=r"^OUTPUT\s*=\s*(?P<header>.+)$", converter=str),
        # 未显式 labels，使用命名组 comp
        Field(label="comp_line", pattern=r"^COMPONENT\s*=\s*(?P<comp>.+)$", converter=str),
        # 显式为每个分组提供 labels
        Field(label="vec", pattern=r"^V:\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)", converter=float, labels=["vx","vy","vz"]),
    ]

    records = parse_text(text, schema, key_end=r"^END$", use_search=False)
    assert len(records) == 2

    r0, r1 = records
    # 命名组应生成为独立键
    assert r0["header"] == "HEADER ONE"
    assert r0["comp"] == "AIRFRAME"
    # 未使用原 label "out"/"comp_line"
    assert "out" not in r0 and "comp_line" not in r0
    # 显式 labels 的多分组向量
    assert r0["vx"] == pytest.approx(1.0)
    assert r0["vy"] == pytest.approx(2.5)
    assert r0["vz"] == pytest.approx(-3.2)

    assert r1["header"] == "HEADER TWO"
    assert r1["comp"] == "ROTOR"
    assert r1["vx"] == pytest.approx(-1.0)
    assert r1["vy"] == pytest.approx(0.0)
    assert r1["vz"] == pytest.approx(3.5)

test_labels_per_group_and_named_groups()