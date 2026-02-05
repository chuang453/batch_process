"""
Test suite for the CLI capabilities feature
Tests the --capabilities and --processors options
"""
import subprocess
import sys
from pathlib import Path

def test_cli_capabilities_option():
    """Test that --capabilities flag works and shows system capabilities"""
    result = subprocess.run(
        [sys.executable, "-m", "cli.app", "--capabilities"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )
    
    # Should exit successfully
    assert result.returncode == 0, f"CLI failed with exit code {result.returncode}"
    
    # Should contain expected headers
    output = result.stdout
    assert "批处理框架能力概览" in output or "What Can I Do" in output, "Missing capability overview header"
    assert "可用处理器列表" in output or "可用处理器" in output, "Missing processor list section"
    
    # Should contain usage examples
    assert "使用示例" in output or "示例" in output, "Missing usage examples"
    
    print("✓ --capabilities option works correctly")


def test_cli_processors_option():
    """Test that --processors flag works and lists processors"""
    result = subprocess.run(
        [sys.executable, "-m", "cli.app", "--processors"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )
    
    # Should exit successfully
    assert result.returncode == 0, f"CLI failed with exit code {result.returncode}"
    
    # Should contain processor list
    output = result.stdout
    assert "可用处理器" in output, "Missing processor list header"
    assert "总计" in output, "Missing processor count"
    
    # Should have at least some processors
    assert "backup_file" in output or "processor" in output.lower(), "No processors found"
    
    print("✓ --processors option works correctly")


def test_cli_help_option():
    """Test that --help shows the new options"""
    result = subprocess.run(
        [sys.executable, "-m", "cli.app", "--help"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )
    
    # Should exit successfully
    assert result.returncode == 0, f"CLI failed with exit code {result.returncode}"
    
    # Should contain new options
    output = result.stdout
    assert "--capabilities" in output, "Missing --capabilities option in help"
    assert "--processors" in output, "Missing --processors option in help"
    
    print("✓ --help shows new options correctly")


def test_capabilities_document_exists():
    """Test that CAPABILITIES_CN.md exists and has content"""
    doc_path = Path(__file__).parent.parent / "CAPABILITIES_CN.md"
    
    assert doc_path.exists(), "CAPABILITIES_CN.md does not exist"
    
    content = doc_path.read_text(encoding='utf-8')
    
    # Check for required sections
    required_sections = [
        "你能干什么",
        "核心能力",
        "处理器系统",
        "配置系统",
        "使用场景"
    ]
    
    for section in required_sections:
        assert section in content, f"Missing required section '{section}' in capabilities document"
    
    print("✓ CAPABILITIES_CN.md exists and has proper content")


def test_readme_updated():
    """Test that README.md was updated with capabilities section"""
    readme_path = Path(__file__).parent.parent / "README.md"
    
    assert readme_path.exists(), "README.md does not exist"
    
    content = readme_path.read_text(encoding='utf-8')
    assert "你能干什么" in content or "What Can I Do" in content, "Missing capabilities section in README"
    assert "--capabilities" in content, "Missing reference to --capabilities in README"
    
    print("✓ README.md contains capabilities information")


if __name__ == "__main__":
    print("Running CLI capabilities tests...\n")
    
    try:
        test_cli_help_option()
        test_cli_processors_option()
        test_cli_capabilities_option()
        test_capabilities_document_exists()
        test_readme_updated()
        
        print("\n✅ All tests passed!")
        sys.exit(0)
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
