import sys
from pathlib import Path
import re

def iter_output_blocks(text: str):
    pattern = re.compile(r"^\s*OUTPUT\s*=\s*(.+?)\s*$", re.MULTILINE)
    for m in pattern.finditer(text):
        start = m.end()
        header = m.group(1).strip()
        # Block ends at next OUTPUT = or end of file
        next_m = pattern.search(text, pos=m.end())
        end = next_m.start() if next_m else len(text)
        block = text[start:end].strip("\n")
        yield header, block


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/extract_output_block.py <file> [--first | --all]")
        sys.exit(1)
    file = Path(sys.argv[1])
    mode_all = "--all" in sys.argv or "--first" not in sys.argv
    text = file.read_text(encoding="utf-8", errors="ignore")
    blocks = list(iter_output_blocks(text))
    if not blocks:
        print("No OUTPUT blocks found.")
        return
    if mode_all:
        for i, (header, block) in enumerate(blocks, 1):
            print(f"===== OUTPUT: {header} (#{i}) =====")
            print(block)
            print()
    else:
        header, block = blocks[0]
        print(f"===== OUTPUT: {header} =====")
        print(block)

if __name__ == "__main__":
    main()
