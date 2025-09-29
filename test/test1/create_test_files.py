# create_test_files.py
from pathlib import Path

root = Path("test_project")
root.mkdir(exist_ok=True)

# 创建测试文件
files = [
    root / "hello.txt",
    root / "world.txt",
    root / "image.jpg",
    root / "data" / "note.txt"
]

for f in files:
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("This is a test file.\n")