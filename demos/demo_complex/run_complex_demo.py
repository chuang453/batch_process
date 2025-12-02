"""Run the complex demo.

This demo demonstrates:
1. Multiple folders with subfolders containing data files
2. Files with 3 types of data (Type1: main data, Type2 & Type3: auxiliary data)
3. Writing folder labels when entering directories
4. Writing data to Word tables (Type1 occupies 2 cells, Type2 & Type3 occupy 1 cell each)
5. Creating individual plots for each file
6. Writing summaries and comprehensive plots when exiting directories

Requirements: matplotlib, python-docx, Pillow
"""
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# Add parent directory to path for imports
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root))

from main import run_pipeline


def main():
    """Run the complex demo."""
    # Paths
    demo_dir = Path(__file__).parent
    root = demo_dir / "data_root"
    config = demo_dir / "complex_config.yaml"
    plugins_dir = demo_dir / "plugins"
    
    print("=" * 70)
    print("复杂演示 - 多层文件夹、三类数据、Word报告与可视化")
    print("=" * 70)
    print(f"\n数据根目录: {root}")
    print(f"配置文件: {config}")
    print(f"插件目录: {plugins_dir}")
    print("\n开始处理...\n")
    
    # Check if required packages are installed
    try:
        import matplotlib
        import docx
        from PIL import Image
        print("✓ 所需包已安装 (matplotlib, python-docx, Pillow)\n")
    except ImportError as e:
        print(f"✗ 缺少必要的包: {e}")
        print("\n请安装所需包:")
        print("  pip install matplotlib python-docx Pillow")
        return
    
    # Load plugins from the demo plugins directory
    from config.loader import load_plugins
    load_plugins(str(plugins_dir))
    
    # Run pipeline
    try:
        ctx = run_pipeline(str(root), str(config))
        
        print("\n" + "=" * 70)
        print("处理完成!")
        print("=" * 70)
        print(f"\n处理结果数: {len(ctx.results)}")
        
        # Show some results
        print("\n前10条结果:")
        for i, r in enumerate(ctx.results[:10], 1):
            action = r.get('action', 'unknown')
            if action == 'enter_folder_label':
                print(f"  {i}. 进入目录: {r.get('label', r.get('folder', 'N/A'))}")
            elif action == 'read_three_type_data':
                print(f"  {i}. 读取文件: {Path(r.get('file', '')).name} "
                      f"(Type1:{r.get('type1_count', 0)}, "
                      f"Type2:{r.get('type2_count', 0)}, "
                      f"Type3:{r.get('type3_count', 0)})")
            elif action == 'exit_folder_summary':
                print(f"  {i}. 离开目录: {r.get('label', r.get('folder', 'N/A'))} "
                      f"(处理{r.get('files_processed', 0)}个文件)")
            else:
                print(f"  {i}. {action}: {r}")
        
        # Check output files
        out_doc = demo_dir / "output_complex.docx"
        img_dir = demo_dir / "images"
        
        print("\n输出文件:")
        print(f"  Word文档: {out_doc} {'✓ 已生成' if out_doc.exists() else '✗ 未找到'}")
        print(f"  图片目录: {img_dir} {'✓ 已生成' if img_dir.exists() else '✗ 未找到'}")
        
        if img_dir.exists():
            images = list(img_dir.glob("*.png"))
            print(f"  生成图片数: {len(images)}")
            for img in images[:5]:
                print(f"    - {img.name}")
            if len(images) > 5:
                print(f"    ... 还有 {len(images) - 5} 张图片")
        
        print("\n" + "=" * 70)
        print("演示完成! 请查看生成的Word文档和图片。")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n处理过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
