# core.py (升级版)
from pathlib import Path
from typing import Callable, Any, Dict, List, Union
import fnmatch
#from dataclasses import dataclass, field
from typing import Generator
import traceback
from decorators.processor import ProcessingContext,PROCESSORS,PRE_PROCESSORS,POST_PROCESSORS
from utils.utils import preorder_tree_paths
Processor = Callable[[Path, ProcessingContext], Any]
'''
#批处理器
# 对目录中的文件或文件夹递归地应用处理函数。
# 基本配置一般用yaml文件写
类型	 写法示例	匹配对象	      说明
🔤 精确文件名	"readme.txt"	文件 readme.txt	区分大小写
📁 目录名（推荐）	"data/"	名为 data 的目录	必须以 / 结尾
🧩 通配符文件	"*.log"	所有 .log 文件	使用 fnmatch 语法
🔁 递归匹配	"**/*.tmp"	所有层级的 .tmp 文件	** 表示任意层级
📂 子目录专用规则	"logs/": { ... }	logs/ 目录下的内容	嵌套配置块

'''


class BatchProcessor:
    def __init__(self, config: Dict = None):  #, processors: Dict[str, Processor]
        self.config = config or {}
        #    self.processors = processors
        self.root_path = None
        self.context = None

        # 提取 pre/post 函数名（从配置中）
  #      self.pre_func_name = self.config.get("pre_process")
   #     self.post_func_name = self.config.get("post_process")

        self.progress_callback = None  ##进度条回调

    def set_config(self, config: Dict):
        self.config = config

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def _call_progress(self, current, total, status):
        if self.progress_callback:
            self.progress_callback(current, total, status)

    def set_worker(self, worker):
        self.worker = worker

    def _is_cancelled(self) -> bool:
        """检查是否被请求取消"""
        if self.worker and self.worker.thread():
            return self.worker.thread().isInterruptionRequested()
        return False



    def run(self, root_path: str | Path, context = None) -> ProcessingContext:
       
     #   self.context = ProcessingContext()
      #  context = self.context
        context = context or ProcessingContext()
        root = Path(root_path)
        context.root_path = root  

        if not root.exists():
            raise FileNotFoundError(f"路径不存在: {root}")
   #     context.set_data('root', root)
        self.root_path = root          ##绝对路径
        print(f"🔍 开始处理: {root}")

        ##收集所有待处理的文件或目录
        # 📂 收集所有 **文件和目录**
    #    all_items = [root] + [p for p in root.rglob("*")]  # 包含 root 自身
        all_items = [root] + preorder_tree_paths(root)  #[p for p in root.rglob("*")]  # 包含 root 自身
        total_items = len(all_items)


        ##前后处理函数
        pre_func_name, config_pre = self._get_pre_config()          
        post_func_name, config_post = self._get_post_config()

        # 🔹 1. 执行初始化函数（如果存在）
        # 统计总操作数
      
        total_steps = (1 if pre_func_name else 0) + sum(
            len(self._get_processors_for_file(p, p.is_dir()))
            for p in all_items) + (1 if post_func_name else 0)
        current_step = 0        
        if pre_func_name:
            print('🚀  开始执行初始化...（{pre_func_name}）')
            if self._is_cancelled():    #取消执行的检查
                return context

            current_step += 1
            self._call_progress(current_step, total_steps,
                                f"🚀 初始化: {pre_func_name}")
            try:
                if pre_func_name in PRE_PROCESSORS:
                    result = PRE_PROCESSORS[pre_func_name](context, **config_pre)
                    context.add_result({"phase": "pre", "result": result})
                    print('✅ 初始化完成!')
                else:
                    print(f"⚠️ 未注册的初始化函数: {pre_func_name}")
        #           self._call_progress(current_step, total_steps, f"⚠️ 跳过初始化: {pre_func_name}")
            except Exception as e:
                print(f"❌ 初始化失败: {e}\n{traceback.format_exc()}")
                return context
        else:
            print(f"⚠️ 未定义初始化函数，跳过")

        # 🔹 2. 遍历所有项（文件 + 目录）
        ii = 0
        for item in all_items:      ##所有待处理项,根目录下所有文件， 绝对路径

            if self._is_cancelled():  # ✅ 每个文件前检查, 执行检查
                self._log("🛑 用户取消，停止处理")
                break        

            is_dir = item.is_dir()

            processors_and_configs = self._get_processors_for_file(
                item, is_dir)

           ## 记录此路径的metadata，一个列表：[处理函数，处理参数，执行情况, 执行排序，警告信息， 错误信息]
         
            rel_path = item.relative_to(root)    ##相对路径
            parts = list(rel_path.parts)
            parts1 = [ai + '/' for ai in parts[:-1]] + [ parts[-1] ] if len(parts) > 0 else ['.']   #键
            metadata_info = [[], [], [], None,[],[]]
            context.set_metadata(parts1, metadata_info)
            
            if not processors_and_configs:
                continue  # 无匹配规则     
                
            ii += 1   ##计数器
            metadata_info[3] = ii

            for processor_name, config in processors_and_configs:
                if self._is_cancelled():  # ✅ 每个处理器前检查
                    break
            
                metadata_info[0].append(processor_name)
                metadata_info[1].append(config)
                current_step += 1
                item_type = "📁目录" if is_dir else "📄文件"
                status = f"{item_type} {item.name} → {processor_name}"
                self._call_progress(current_step, total_steps, status)
                print(status)

                if processor_name in PROCESSORS:  #AVAILABLE_PROCESSORS
                    try:
                        # ✅ 把 config 作为 context 的一部分传入
                        # 建议：context.config = config，或作为参数

                        func = PROCESSORS[processor_name]              

                        result = PROCESSORS[processor_name](item, context,
                                                            **config)
                        context.add_result({
                            "phase": "item",
                            "path": str(item),
                            "type": "dir" if is_dir else "file",
                            "processor": processor_name,
                            "config": config,
                            "result": result
                        })
                        
                        metadata_info[2].append('succeed')
                    except Exception as e:
                        print(f"❌ 处理失败 [{processor_name} on {item}]: {e}\n{traceback.format_exc()}")
                        context.add_result({
                            "error": str(e),
                            "processor": processor_name,
                            "path": str(item)
                        })
                        metadata_info[2].append('failed')  ##
                        metadata_info[4].append(f'processor_name: {e}')    ##错误信息
                else:
                    print(f"⚠️ 未注册处理器: {processor_name}")
                    metadata_info[2].append('failed')
                    metadata_info[4].append(f'processor_name: 未注册处理器')    ##错误信息

    # 🔹 3. post_process
        if not self._is_cancelled() and post_func_name:
            current_step += 1
            self._call_progress(current_step, total_steps,
                                f"🏁 最终处理: {post_func_name}")
            try:
                if post_func_name in POST_PROCESSORS:
                    result = POST_PROCESSORS[post_func_name](context,**config_post)
                    context.add_result({"phase": "post", "result": result})
            except Exception as e:
                print(f"❌ 最终处理失败: {e}\n{traceback.format_exc()}")
    #    self.context = context
        return context

    #获取前处理器需要的参数config
    def _get_pre_config(self):
        func_name = self.config.get('pre_process')
        config = self.config.get('config_pre', {})
        return [func_name, config]

    #获取后处理器需要的参数config
    def _get_post_config(self):
        func_name = self.config.get('post_process')
        config = self.config.get('config_post', {})
        return [func_name, config]
    
  #      return self.config.get('config_post', {})

    #获取单个文件对应的处理函数processor和额外输入参数config
    def _get_processors_for_file(self,
                                 path: Path,
                                 is_dir: bool = False
                                 ) -> list[tuple[str, dict]]:
        """
        返回该路径匹配的处理器及其配置
        返回格式: [(processor_name, config), ...]
        """
        matched_rules = []

        # 获取相对root_path的相对路径（统一用 /）
 #       try:
 #           rel_path = path.relative_to(self.root_path).as_posix()
 #       except ValueError:
 #           rel_path = path.name  # fallback

        # 遍历所有规则， 对每个匹配模式的
        for pattern, rule in self.config.items():
            if pattern in ("pre_process", "post_process","config_pre","config_post"):   ##排除前后处理相关的参数
                continue
            if not isinstance(rule, dict) or "processors" not in rule:
                continue  # 兼容旧格式？可选

            # 检查是否匹配
            if self._match_rule(path, pattern, is_dir):
                priority = rule.get("priority", 5)  # 默认优先级 5
                config = rule.get("config", {})
                must_excute = rule.get("must_excute", False)
                processors = rule["processors"]

                for proc in processors:
                    matched_rules.append({
                        "processor": proc,
                        "config": config,
                        "priority": priority,
                        "must_excute": must_excute
                    })

        # 按优先级降序（高优先级在前）
        matched_rules.sort(key=lambda x: x["priority"], reverse=True)   #按priority从大到小排列

        # 🔥 只返回最高优先级的处理器（防止重复处理）
        if not matched_rules:
            return []

        matched_rules1 = [x for x in matched_rules if not x["must_excute"]]   ##
        if matched_rules1:
            highest_prio = matched_rules1[0]["priority"]   ##从不是must_excute的处理函数中选取优先级最大的数
            top_rules = [r for r in matched_rules if r["priority"] == highest_prio or r["must_excute"]]   ##保留priority最大，或必须执行的processor，按priority排序。
        else:
            top_rules = [r for r in matched_rules if r["must_excute"]] 

        return [(r["processor"], r["config"]) for r in top_rules]

    def _match_rule(self,
                    path: Path,
                    pattern: str,
                    is_dir: bool = False) -> bool:
        rel_path = path.relative_to(self.root_path).as_posix()

        if pattern.endswith('/'):   ##匹配文件夹
            # 目录前缀匹配：data/ → 匹配 data文件夹
            return fnmatch.fnmatch(rel_path, pattern.rstrip('/')) and is_dir    #rel_path.startswith(pattern.rstrip('/'))
        else:
            # 通配符匹配：*.txt, logs/**/*.log
            return fnmatch.fnmatch(rel_path, pattern)

    # 示例：获取当前启用的处理器
    def get_enabled_processors(self):
        enabled = {}
        for row in range(self.plugin_table.rowCount()):
            cb = self.plugin_table.item(row, 1)
            if cb.checkState() == Qt.Checked and hasattr(cb, 'plugin_func'):
                func = cb.plugin_func
                enabled[func.processor_name] = func
        return enabled
