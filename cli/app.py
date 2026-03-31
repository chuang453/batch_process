# cli.py
# 命令行工具：python -m batch_processor.cli ./data -c config.yaml
import argparse

from config.loader import load_config, generate_template, load_plugins, is_pipeline_config
from core.engine import BatchProcessor
from core.pipeline import Pipeline
from decorators.processor import AVAILABLE_PROCESSORS

def main():
    parser = argparse.ArgumentParser(description="递归批处理系统")
    parser.add_argument("root", nargs="?", help="目标目录路径")
    parser.add_argument("-c", "--config", help="配置文件路径 (JSON/YAML)")
    parser.add_argument("--generate-template",
                        nargs="?",
                        const="config.yaml",
                        help="生成配置模板")
    parser.add_argument("--processors", action="store_true", help="列出可用处理器")
    parser.add_argument("--simulate-pipeline",
                        action="store_true",
                        help="Pipeline 配置下输出模拟步骤")

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

    load_plugins()
    config = load_config(args.config)

    if is_pipeline_config(config):
        pipeline = Pipeline(stages=config.get('pipeline', []))
        if args.simulate_pipeline:
            sim = pipeline.simulate(root_path=args.root)
            print(f"Pipeline total steps: {sim.get('total_steps', 0)}")
            for step in sim.get('steps', []):
                print(step)
            return
        pipeline.run(root_path=args.root)
        return

    processor = BatchProcessor(config)
    processor.run(args.root)

if __name__ == "__main__":
    main()