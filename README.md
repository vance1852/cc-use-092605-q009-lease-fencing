# 修复过期分析租约仍可提交结果基础平台

本项目是一套可离线运行的 Python 服务端平台，用于管理海上风电场、送出通道、机组资源批次、场站申报、功率分配、调度情景与机组健康准入。业务状态、登录权限、幂等结果和审计事件保存在 SQLite 中，适合生产调度、设备质量、风险与审计人员在单个 Linux 应用容器内协作。

## 目录

- `src/wind_dispatch/`：场站、送出通道、机组可用量、功率申报、日前分配和调度情景；
- `src/turbine_health/`：机组健康协议、测点导入、异常复核、分析任务租约和健康决定；
- `src/grid_qualification/`：并网机组批次、检测数据、分析、账号登录与质量审批；
- `fixtures/`：离线验收使用的检测协议和结构化测点；
- `tests/`：核心规则、权限、错误边界、事务、API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m wind_dispatch.acceptance --workspace .
PYTHONPATH=src python3 -m turbine_health.acceptance --workspace .
PYTHONPATH=src python3 -m grid_qualification.acceptance
```

三条命令使用临时 SQLite 数据库完成场站与通道登记、功率申报分配、健康测点分析和并网审批，不访问外部网络。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m wind_dispatch.api --database wind.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m turbine_health.api --database health.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m grid_qualification.api --database grid.sqlite3 --host 127.0.0.1 --port 8082
```

服务均提供 `GET /health`，其余接口使用 JSON。账号登录和角色权限由服务端校验，进程重启后可以继续查询 SQLite 中的业务状态与审计历史。

## 分析任务租约栅栏

`turbine_health` 的分析队列在提交边界核对五道栅栏：当前领取者、租约代次、有效期限、任务修订号和测点输入摘要。工作进程通过 `POST /jobs/claim` 领取任务（响应含 `lease_generation`、`attempts` 与领取时的 `input_sha256`），提交时必须回传该代次：

- `POST /jobs/{id}/complete`：请求体携带 `worker_id` 与 `lease_generation`。任何一项栅栏落后都返回 `409 lease_conflict`，且不产生分析记录、状态变更或审计事件；同一持有者对同一代租约重复提交相同结果会取回原响应。
- `POST /jobs/{id}/fail`：请求体携带 `worker_id`、`lease_generation`、`error`，栅栏与完成一致，迟到上报不会覆盖新持有者的状态。
- `GET /jobs/{id}/history`：审计角色查询各次领取、接管、失败与最终落库使用的测点输入摘要；批次报告 `GET /batches/{id}/report` 同样包含每个任务的生命周期事件。
