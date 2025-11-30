# logstruct/lark_compiler.py
# logstruct/schema.py
from typing import List, Optional, Any, Callable, Union
import re

class Field:
    def __init__(self, name: str, pattern: str, converter: Callable = None):
        self.name = name
        self.pattern = pattern
        self.converter = converter or (lambda x: x)

class Repeat:
    def __init__(self, name: str, items: List['ParseNode'], until: Optional[str] = None):
        self.name = name
        self.items = items
        self.until = until  # 正则字符串，用于识别结束标记

ParseNode = Union[Field, Repeat]
Schema = List[ParseNode]

class Block:
    def __init__(self, name: str, items: List[ParseNode], end_marker: Optional[str] = None):
        self.name = name
        self.items = items
        self.end_marker = end_marker  # 如 "END ROTOR"

import re
from typing import List, Dict, Any, Union
from lark import Lark, Transformer
#from .schema import Field, Repeat, Block, ParseNode

class LarkSchemaCompiler:
    def __init__(self):
        self.rules: Dict[str, str] = {}
        self.transformer_methods: Dict[str, callable] = {}
        self.rule_counter = 0

    def _make_rule_name(self, prefix: str = "rule") -> str:
        name = f"{prefix}_{self.rule_counter}"
        self.rule_counter += 1
        return name

    def _escape_literal(self, s: str) -> str:
        # 转义 Lark 字面量中的特殊字符
        return repr(s)

    def _regex_to_lark_token(self, pattern: str) -> str:
        # 将用户正则包装为 Lark token（注意：需确保不冲突）
        # 我们用 TOKEN_0, TOKEN_1... 避免命名冲突
        token_name = f"TOKEN_{self.rule_counter}"
        self.rule_counter += 1
        # 在 grammar 中定义为：TOKEN_X: /pattern/
        self.rules[token_name] = f'/{pattern}/'
        return token_name

    def compile_field(self, field: Field) -> str:
        # Field -> 单个 token + 可选转换
        token = self._regex_to_lark_token(field.pattern)
        rule_name = self._make_rule_name(f"field_{field.name}")

        # 规则：field_xxx: TOKEN_N
        self.rules[rule_name] = f"{token}"

        # 注册 transformer 方法
        def transform_method(args):
            value = args[0].value  # Token value
            return field.converter(value) if field.converter else value

        self.transformer_methods[rule_name] = transform_method
        return rule_name

    def compile_repeat(self, repeat: Repeat) -> str:
        # Repeat -> ZeroOrMore(child_rule)
        child_rules = []
        for item in repeat.items:
            child_rule = self.compile_node(item)
            child_rules.append(child_rule)

        # 合并子节点为一个序列规则
        seq_rule_name = self._make_rule_name("seq")
        seq_body = " ".join(child_rules)
        self.rules[seq_rule_name] = seq_body

        # 主规则：repeat_xxx: seq_rule*
        rule_name = self._make_rule_name(f"repeat_{repeat.name}")
        self.rules[rule_name] = f"{seq_rule_name}*"

        # Transformer: 收集所有子结果为列表
        def transform_method(args):
            # args 是 [Tree(seq), Tree(seq), ...]
            return [arg.children[0] if len(arg.children) == 1 else arg for arg in args]

        self.transformer_methods[rule_name] = transform_method
        return rule_name

    def compile_block(self, block: Block) -> str:
        # Block -> sequence of items, optionally stop at end_marker
        child_rules = []
        for item in block.items:
            child_rules.append(self.compile_node(item))

        seq_rule = self._make_rule_name("block_seq")
        self.rules[seq_rule] = " ".join(child_rules)

        rule_name = self._make_rule_name(f"block_{block.name}")

        if block.end_marker:
            end_token = self._regex_to_lark_token(block.end_marker)
            self.rules[rule_name] = f"{seq_rule} {end_token}?"
        else:
            self.rules[rule_name] = seq_rule

        # Transformer: 返回字典（按子节点 name 聚合）
        def transform_method(args):
            result = {}
            # 注意：args 顺序与 schema 一致
            i = 0
            for item in block.items:
                if isinstance(item, Field):
                    result[item.name] = args[i]
                    i += 1
                elif isinstance(item, (Repeat, Block)):
                    result[item.name] = args[i]
                    i += 1
            return result

        self.transformer_methods[rule_name] = transform_method
        return rule_name

    def compile_node(self, node: ParseNode) -> str:
        if isinstance(node, Field):
            return self.compile_field(node)
        elif isinstance(node, Repeat):
            return self.compile_repeat(node)
        elif isinstance(node, Block):
            return self.compile_block(node)
        else:
            raise TypeError(f"Unknown node type: {type(node)}")

    def compile_schema(self, schema: List[ParseNode]) -> tuple[str, type]:
        # 入口：start -> all top-level nodes
        top_rules = []
        for node in schema:
            top_rules.append(self.compile_node(node))

        seq_rule = self._make_rule_name("top_seq")
        self.rules[seq_rule] = " ".join(top_rules)

        self.rules["start"] = f"{seq_rule}+"

        # 构建 grammar 字符串
        grammar_lines = []
        for name, body in self.rules.items():
            if name.startswith("TOKEN_"):
                grammar_lines.append(f"{name.upper()}: {body}")
            else:
                grammar_lines.append(f"{name}: {body}")

        # 添加通用规则
        grammar_lines.extend([
            "%import common.WS",
            "%ignore WS",
            "_NL: /\\\\r?\\\\n/",
            "%ignore _NL"
        ])

        grammar = "\n".join(grammar_lines)

        # 动态创建 Transformer 类
        methods = {"__module__": __name__}
        methods.update(self.transformer_methods)

        DynamicTransformer = type("DynamicLogStructTransformer", (Transformer,), methods)

        return grammar, DynamicTransformer
    
# example.py
#from logstruct.schema import Field, Repeat, Block
#from logstruct.lark_compiler import LarkSchemaCompiler

# 定义 schema
rotor_schema = [
    Block("rotor", [
        Field("rotor_id", r"ROTOR\s+(\d+)", int),
        Field("radius", r"RADIUS\s+\(M\)\s*=\s*([\d.]+)", float),
        Repeat("stats", [
            Field("stat_type", r"(MEAN|MAXIMUM|MINIMUM)"),
            Field("values", r"([\d.\s]+)", lambda s: [float(x) for x in s.split()])
        ], until=r"PSI\s+="),
        Field("psi", r"PSI\s*=\s*([\d.]+)", float)
    ], end_marker=r"")
]

# 编译
compiler = LarkSchemaCompiler()
grammar, TransformerClass = compiler.compile_schema(rotor_schema)

print("=== Generated Grammar ===")
print(grammar)
print("\n=== Parsing ===")

# 测试日志
log_text = """
ROTOR 1
RADIUS (M) = 5.2
MEAN 100.0 200.0 300.0 400.0
MAXIMUM 150.0 250.0 350.0 450.0
PSI = 0.85

ROTOR 2
RADIUS (M) = 6.1
MEAN 110.0 210.0 310.0 410.0
PSI = 0.90
"""

# 解析
parser = Lark(grammar, parser="lalr")
tree = parser.parse(log_text)
result = TransformerClass().transform(tree)

print(result)