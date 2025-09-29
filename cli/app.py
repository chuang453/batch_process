# cli.py
# 命令行工具：python -m batch_processor.cli run ./data --config config.yaml --output report.csv
import argparse
from core import BatchProcessor
from config import load_config, generate_template, AVAILABLE_PROCESSORS

def main():
    parser = argparse.ArgumentParser(description="递归批处理系统")
    parser.add_argument("root", nargs="?", help="目标目录路径")
    parser.add_argument("-c", "--config", help="配置文件路径 (JSON/YAML)")
    parser.add_argument("--generate-template", nargs="?", const="config.yaml", help="生成配置模板")
    parser.add_argument("--processors", action="store_true", help="列出可用处理器")

    args = parser.parse_args()

    if args.processors:
        print("可用处理器:")
        for name in AVAILABLE_PROCESSORS:
            print(f"  - {name}")
        return

    if args.generate_template:
        generate_template(args.generate_template)
        return

    if not args.root or not args.config:
        parser.print_help()
        return

    config = load_config(args.config)
    processor = BatchProcessor(config, AVAILABLE_PROCESSORS)
    processor.run(args.root)

if __name__ == "__main__":
    main()