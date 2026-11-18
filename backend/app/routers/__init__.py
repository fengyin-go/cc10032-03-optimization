"""业务路由汇总：通用资源路由 + 结果落地恢复专用路由。"""
from __future__ import annotations

from app.routers import resource, sink_recovery

# sink_recovery 全部为 /api/sink-recovery/* 固定前缀，
# 与 /api/{module} 动态资源路由不冲突，顺序注册即可。
ROUTERS = [sink_recovery, resource]
