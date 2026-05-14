"""
Remote Claude 使用统计模块

全局接口：
  track(category, event, **kwargs)   —— 记录事件到本地 SQLite
  close()                            —— 关闭前刷新
"""

from .collector import StatsCollector

_collector = StatsCollector()


def track(category: str, event: str, **kwargs) -> None:
    """记录事件（非阻塞，线程安全，异常不传播）"""
    _collector.track(category, event, **kwargs)


def close() -> None:
    """关闭前刷新队列"""
    _collector.close()
