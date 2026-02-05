# cli.py
# 命令行工具：python -m batch_processor.cli run ./data --config config.yaml --output report.csv
import argparse
import sys
from pathlib import Path
from core.engine import BatchProcessor
from config.loader import load_config, generate_template, load_plugins
from decorators.processor import get_all_processors

def show_capabilities():
    """显示系统能力和可用处理器的详细信息"""
    print("=" * 80)
    print("批处理框架能力概览 - What Can I Do?")
    print("=" * 80)
    print()
    print("本框架是一个递归批处理系统，支持:")
    print("  ✅ 根据 YAML/JSON 配置自动处理文件和目录")
    print("  ✅ 灵活的模式匹配（支持 **、通配符、目录/文件区分）")
    print("  ✅ 可配置的处理器优先级和执行顺序")
    print("  ✅ 丰富的内置处理器和插件系统")
    print("  ✅ GUI 和 CLI 两种使用方式")
    print("  ✅ 处理结果记录和历史查询")
    print()
    print("-" * 80)
    print("可用处理器列表:")
    print("-" * 80)
    
    # Load built-in processors
    try:
        import processors
    except Exception as e:
        print(f"⚠️  警告: 加载内置处理器时出错: {e}")
    
    # Load plugins if plugins directory exists
    plugins_dir = Path("plugins")
    if plugins_dir.exists():
        try:
            load_plugins(str(plugins_dir))
        except Exception as e:
            print(f"⚠️  警告: 加载插件时出错: {e}")
    
    procs = get_all_processors()
    
    if not procs:
        print("  (无可用处理器)")
        return
    
    # Group by kind
    pre_procs = [p for p in procs if p['kind'] == 'pre']
    file_procs = [p for p in procs if p['kind'] == 'file']
    post_procs = [p for p in procs if p['kind'] == 'post']
    
    if pre_procs:
        print()
        print("📋 前处理器 (Pre-processors) - 在处理前执行:")
        for p in pre_procs:
            meta = p.get('metadata', {})
            desc = meta.get('description', '')
            author = meta.get('author', '')
            version = meta.get('version', '')
            
            info_parts = []
            if desc:
                info_parts.append(desc)
            if author:
                info_parts.append(f"作者: {author}")
            if version:
                info_parts.append(f"版本: {version}")
            
            info_str = " | ".join(info_parts) if info_parts else "无描述"
            print(f"  • {p['name']:30} (优先级: {p['priority']:3}) - {info_str}")
    
    if file_procs:
        print()
        print("📁 文件/目录处理器 (File/Dir Processors) - 处理具体路径:")
        for p in file_procs:
            meta = p.get('metadata', {})
            desc = meta.get('description', '')
            author = meta.get('author', '')
            version = meta.get('version', '')
            
            info_parts = []
            if desc:
                info_parts.append(desc)
            if author:
                info_parts.append(f"作者: {author}")
            if version:
                info_parts.append(f"版本: {version}")
            
            info_str = " | ".join(info_parts) if info_parts else "无描述"
            print(f"  • {p['name']:30} (优先级: {p['priority']:3}) - {info_str}")
    
    if post_procs:
        print()
        print("📊 后处理器 (Post-processors) - 在处理后执行:")
        for p in post_procs:
            meta = p.get('metadata', {})
            desc = meta.get('description', '')
            author = meta.get('author', '')
            version = meta.get('version', '')
            
            info_parts = []
            if desc:
                info_parts.append(desc)
            if author:
                info_parts.append(f"作者: {author}")
            if version:
                info_parts.append(f"版本: {version}")
            
            info_str = " | ".join(info_parts) if info_parts else "无描述"
            print(f"  • {p['name']:30} (优先级: {p['priority']:3}) - {info_str}")
    
    print()
    print("=" * 80)
    print(f"总计: {len(procs)} 个处理器 (前: {len(pre_procs)}, 文件: {len(file_procs)}, 后: {len(post_procs)})")
    print("=" * 80)
    print()
    print("使用示例:")
    print("  查看帮助:        python -m cli.app --help")
    print("  生成配置模板:    python -m cli.app --generate-template config.yaml")
    print("  运行批处理:      python -m cli.app <目录> -c config.yaml")
    print("  启动 GUI:        python main.py")
    print()

def main():
    parser = argparse.ArgumentParser(
        description="递归批处理系统 - 根据配置文件自动处理文件和目录",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  查看系统能力:        python -m cli.app --capabilities
  列出所有处理器:      python -m cli.app --processors
  生成配置模板:        python -m cli.app --generate-template
  运行批处理:          python -m cli.app ./data -c config.yaml
        """
    )
    parser.add_argument("root", nargs="?", help="目标目录路径")
    parser.add_argument("-c", "--config", help="配置文件路径 (JSON/YAML)")
    parser.add_argument("--generate-template", nargs="?", const="config.yaml", help="生成配置模板")
    parser.add_argument("--processors", action="store_true", help="列出可用处理器（简略）")
    parser.add_argument("--capabilities", action="store_true", help="显示系统能力和处理器详情")

    args = parser.parse_args()

    if args.capabilities:
        show_capabilities()
        return
    
    if args.processors:
        # Load processors
        try:
            import processors
        except Exception as e:
            print(f"⚠️  警告: 加载内置处理器时出错: {e}")
        
        plugins_dir = Path("plugins")
        if plugins_dir.exists():
            try:
                load_plugins(str(plugins_dir))
            except Exception as e:
                print(f"⚠️  警告: 加载插件时出错: {e}")
        
        procs = get_all_processors()
        print("可用处理器:")
        for p in procs:
            print(f"  • {p['name']:35} [{p['kind']:4}] (优先级: {p['priority']:3})")
        print(f"\n总计: {len(procs)} 个处理器")
        print("\n提示: 使用 --capabilities 查看详细信息")
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