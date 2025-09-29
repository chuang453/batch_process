##一些工具函数
from pathlib import Path




##文件夹和文件的排序， 严格树状展开排序
def preorder_tree_paths(root: Path):
    result = []
    def dfs(p):
        result.append(p)
        if p.is_dir():
            try:
                items = sorted(p.iterdir())
                dirs = [x for x in items if x.is_dir()]
                files = [x for x in items if x.is_file()]
                for d in dirs:
                    dfs(d)
                for f in files:
                    result.append(f)
            except PermissionError:
                pass
    dfs(root)
    return result

