#!/usr/bin/python3
"""
clang-format批量格式化C++文件脚本
支持递归遍历目录，格式化所有C++源文件和头文件
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


class ClangFormatFormatter:
    """C++代码格式化工具类"""
    
    # C++文件扩展名
    CPP_EXTENSIONS = {'.cpp', '.cc', '.cxx', '.c++'}
    HEADER_EXTENSIONS = {'.h', '.hpp', '.hxx', '.h++'}
    ALL_EXTENSIONS = CPP_EXTENSIONS | HEADER_EXTENSIONS
    
    def __init__(self, root_dir: str = ".", clang_format_path: str = "clang-format"):
        """
        初始化格式化工具
        
        Args:
            root_dir: 要格式化的根目录
            clang_format_path: clang-format可执行文件路径
        """
        self.root_dir = Path(root_dir).resolve()
        self.clang_format_path = clang_format_path
        self.formatted_files: List[str] = []
        self.error_files: List[Tuple[str, str]] = []
        
    def find_cpp_files(self) -> List[Path]:
        """
        递归查找所有C++文件
        
        Returns:
            C++文件路径列表
        """
        cpp_files = []
        
        for root, dirs, files in os.walk(self.root_dir):
            # 跳过.git等隐藏目录
            dirs[:] = [d for d in dirs if not d.startswith('.') and d != 'build']
            
            for file in files:
                ext = Path(file).suffix.lower()
                if ext in self.ALL_EXTENSIONS:
                    cpp_files.append(Path(root) / file)
                    
        return sorted(cpp_files)
    
    def format_file(self, file_path: Path) -> bool:
        """
        格式化单个文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            是否成功格式化
        """
        try:
            # 检查clang-format是否存在
            result = subprocess.run(
                [self.clang_format_path, "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode != 0:
                print(f"❌ clang-format不可用: {result.stderr.strip()}")
                return False
            
            # 读取原始内容
            with open(file_path, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            # 调用clang-format
            process = subprocess.Popen(
                [self.clang_format_path, "-style=file", "-i", str(file_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = process.communicate(timeout=30)
            
            if process.returncode == 0:
                self.formatted_files.append(str(file_path))
                return True
            else:
                error_msg = stderr.decode('utf-8').strip() if stderr else "未知错误"
                self.error_files.append((str(file_path), error_msg))
                return False
                
        except subprocess.TimeoutExpired:
            self.error_files.append((str(file_path), "格式化超时"))
            return False
        except FileNotFoundError:
            self.error_files.append((str(file_path), "文件不存在"))
            return False
        except Exception as e:
            self.error_files.append((str(file_path), str(e)))
            return False
    
    def format_all(self, dry_run: bool = False) -> dict:
        """
        格式化所有找到的C++文件
        
        Args:
            dry_run: 是否仅预览不实际修改
            
        Returns:
            格式化结果统计
        """
        print(f"🔍 开始扫描目录: {self.root_dir}")
        
        cpp_files = self.find_cpp_files()
        
        if not cpp_files:
            print("⚠️  未找到任何C++文件")
            return {
                "total": 0,
                "formatted": 0,
                "errors": 0,
                "success_rate": 0.0
            }
        
        print(f"📁 找到 {len(cpp_files)} 个C++文件\n")
        
        success_count = 0
        error_count = 0
        
        for i, file_path in enumerate(cpp_files, 1):
            relative_path = file_path.relative_to(self.root_dir)
            status = "🔄 " if not dry_run else "👀 "
            print(f"{status}[{i}/{len(cpp_files)}] {relative_path}", end=" ")
            
            if dry_run:
                print("✓ (预览模式)")
                success_count += 1
            else:
                if self.format_file(file_path):
                    print("✓")
                    success_count += 1
                else:
                    print("✗")
                    error_count += 1
        
        total = len(cpp_files)
        success_rate = (success_count / total * 100) if total > 0 else 0
        
        return {
            "total": total,
            "formatted": success_count,
            "errors": error_count,
            "success_rate": success_rate
        }
    
    def print_summary(self, stats: dict):
        """打印格式化摘要"""
        print("\n" + "="*60)
        print("📊 格式化完成")
        print("="*60)
        print(f"总文件数:     {stats['total']}")
        print(f"成功格式化:   {stats['formatted']}")
        print(f"失败文件:     {stats['errors']}")
        print(f"成功率:       {stats['success_rate']:.1f}%")
        
        if self.error_files:
            print("\n❌ 失败的文件:")
            for file_path, error in self.error_files:
                print(f"  - {file_path}: {error}")
        
        print("="*60)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="批量格式化C++文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                          # 格式化当前目录
  %(prog)s --dir ./src              # 指定目录
  %(prog)s --dry-run                # 预览模式
  %(prog)s --clang-format /usr/bin/clang-format  # 指定clang-format路径
        """
    )
    
    parser.add_argument(
        "-d", "--dir", 
        default=".",
        help="要格式化的目录 (默认: 当前目录)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="预览模式，不实际修改文件"
    )
    
    parser.add_argument(
        "--clang-format",
        default="clang-format",
        help="clang-format可执行文件路径 (默认: clang-format)"
    )
    
    args = parser.parse_args()
    
    # 创建格式化工具
    formatter = ClangFormatFormatter(
        root_dir=args.dir,
        clang_format_path=args.clang_format
    )
    
    # 执行格式化
    stats = formatter.format_all(dry_run=args.dry_run)
    
    # 打印摘要
    formatter.print_summary(stats)
    
    # 返回退出码
    sys.exit(0 if stats['errors'] == 0 else 1)


if __name__ == "__main__":
    main()
