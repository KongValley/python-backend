# 轻量 Python 后端

单文件 HTTP/JSON 服务，使用 Python 标准库和内置 SQLite。相同源码可由 Windows x86（32 位）和 x64（64 位）的 Python 运行，无需安装第三方包。代码兼容 Python 3.8，部署时请选择目标系统支持、仍在维护且位数匹配的版本。[Python 官方 Windows 下载页](https://www.python.org/downloads/windows/)

## 启动

在此目录打开 PowerShell，运行：

```powershell
py .\server.py
```

服务默认监听 `127.0.0.1:8000`，接受 `127.0.0.1` 或 `localhost` 作为请求主机。首次启动在脚本目录创建 `items.sqlite3`，重启后仍可访问已保存的数据。旧库在启动时自动补充更新时间字段。按 `Ctrl+C` 停止服务。也可以用已安装的 `python.exe` 替代 `py`。

可通过参数配置端口和数据库文件。数据库文件的父目录需要存在且可写。

```powershell
py .\server.py --port 8080 --db .\my-data.sqlite3
```

## 控制面板

双击目录中的 `panel.bat`，或在 PowerShell 中运行：

```powershell
py .\server.py --panel
```

面板可以设置端口、数据库文件、日志文件和日志模式。点击「启动」后服务开始监听，状态变为运行中；点击「停止」会优雅关闭服务。关闭面板窗口也会停止服务。

运行日志按级别（信息、成功、警告、错误）和关键词筛选，日志行同时按级别着色。

面板使用 Python 自带的 Tkinter，无需安装第三方包。面板启动的服务沿用命令行传入的 `--cors-origin`、`--rate-limit`、`--max-connections` 等参数和 `PYTHON_BACKEND_TOKEN` 环境变量。`panel.bat` 使用 `pyw` 启动，不显示控制台窗口；需要查看终端输出时可直接运行 `py .\server.py --panel`。

## 并发与锁等待

可在启动时启用 WAL，让查询在另一个事务写入时继续读取已提交的数据。SQLite 同一时间只能执行一个写事务。锁等待超时后，HTTP 接口返回 `503`。

```powershell
py .\server.py --journal-mode wal --busy-timeout-ms 5000
```

`--busy-timeout-ms` 默认 2000 毫秒，范围 0–60000，设为 0 表示遇锁立即返回。新库默认使用 `delete`，已有库默认沿用自身模式。WAL 设置保存在数据库中。需要切回时，先关闭使用该库的服务和工具，再以 `--journal-mode delete` 启动。WAL 数据库应放在本地磁盘，运行期间可能出现 `-wal` 和 `-shm` 文件。

服务默认最多同时处理 32 个连接，可用 `--max-connections` 调整，范围 1–256。达到上限时，新连接在内核监听队列中等待，直到其他连接结束。

## 自检与回归测试

首次部署时，先用计划上线的 Python、端口和路径运行自检，通过后再启动。数据库父目录需提前创建。日志目录还不存在时，自检会探测最近的已有父目录。

```powershell
$config = @('--port', '8080', '--db', '.\my-data.sqlite3', '--log-file', '.\logs\my-server.log', '--journal-mode', 'wal', '--busy-timeout-ms', '5000')
py .\server.py --check @config
if ($LASTEXITCODE -ne 0) { throw "自检失败，退出码 $LASTEXITCODE" }
py .\server.py @config
```

`--check` 输出 Python 版本和解释器位数、SQLite 版本、数据库和日志目录可写性，以及 `127.0.0.1` 上目标 TCP 端口的绑定结果。端口默认为 `8000`，也可用 `--port` 指定。自检只做短暂绑定，无需启动 HTTP 服务。遇到占用、权限错误或操作结果不符时，会打印失败步骤和原因并返回非零退出码。全部通过返回 `0`。

自检以只读 SQL 连接校验已有数据库的完整性和表结构。SQLite 打开已有 WAL 数据库时可能在其旁边生成 `-wal` 和 `-shm` 辅助文件。新增、列表、读取、替换、删除、源库连接保持打开时的在线备份和恢复都在同一数据库目录下的临时子目录中完成，并校验读回内容。临时数据库、快照和恢复文件会在退出时清理。

使用全新临时路径验证时，可在 PowerShell 中运行下列命令。可将 `py` 换成 `py -3.8-32` 或 `py -3.8-64`，以验证指定的已安装解释器。

```powershell
$checkDir = Join-Path ([IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString())
[void][IO.Directory]::CreateDirectory($checkDir)
try {
    py .\server.py --check --db (Join-Path $checkDir 'probe.sqlite3') --log-file (Join-Path $checkDir 'logs\server.log')
    if ($LASTEXITCODE -ne 0) { throw "自检失败，退出码 $LASTEXITCODE" }
} finally {
    [IO.Directory]::Delete($checkDir)
}
```

`py -B -m unittest discover -v` 使用独立的临时数据库和本地临时端口运行回归测试，覆盖自检、HTTP 接口、旧库升级、写锁竞争、WAL 在线备份与恢复、日志轮转和交互退出。

## 交互控制台

用 `--interactive` 或 `-i` 启动。看到 `backend>` 后可输入命令，HTTP 接口同时接受请求。

```powershell
py .\server.py --interactive
```

| 命令 | 用途 |
| --- | --- |
| `help` 或 `help backup` | 列出可用命令或查看具体用法 |
| `status` | 查看监听地址、数据库状态和日志路径 |
| `list` 或 `list 5` | 按 ID 查看条目摘要，包含名称和状态 |
| `list 5 12` | 查看 ID 大于 12 的前 5 条，也可直接输入控制台给出的下一页命令 |
| `show 12` | 查看条目 12 的完整 JSON |
| `backup ".\my backup.sqlite3"` | 在线备份到新文件 |
| `exit` | 停止服务并返回终端，也可按 `Ctrl+C` |

## 接口

| 方法与路径 | 用途 |
| --- | --- |
| `GET /health` | 检查数据库可读性。正常时返回 `{"status":"ok"}`，数据库不可用时返回 `503` |
| `POST /api/echo` | 接收 UTF-8 JSON 并返回相同数据 |
| `POST /api/items` | 保存 JSON 对象，返回 `201`、新条目和 `Location` 响应头 |
| `GET /api/items` | 分页列出条目，支持名称搜索、状态筛选和排序 |
| `GET /api/items/{id}` | 读取指定条目 |
| `PUT /api/items/{id}` | 完整替换指定条目的 JSON 对象 |
| `DELETE /api/items/{id}` | 删除指定条目，返回 `204` |

写入请求使用 `Content-Type: application/json`，请求体上限为 64 KiB。新增、读取和替换返回 `{"id":1,"data":{"name":"example"}}` 形式的条目。列表返回 `items` 和 `next_after`。接口错误返回 `{"error":"错误代码"}`。

| 列表参数 | 用途 |
| --- | --- |
| `name` | 按 `data.name` 字符串做包含匹配，忽略大小写 |
| `status` | 按 `data.status` 字符串做精确匹配 |
| `sort` | `id`（默认）或 `-id` 按 ID 升序或降序。`updated_at` 或 `-updated_at` 按更新时间升序或降序，相同时间按 ID 排序 |
| `limit` | 每页条目数，默认 20，范围 1–50 |
| `after` | 使用上一页返回的 `next_after`，同时保持原筛选和排序参数。`next_after` 为 `null` 时已到末页 |

名称和状态参数可组合使用，各最多 256 个字符。按更新时间排序时，每项额外返回 `updated_at`，单位为 Unix 毫秒，升级前的条目在首次更新前返回 `null`，排序时按最早时间处理。更新时间排序的 `next_after` 是字符串，按 ID 排序时为数字。

在另一个 PowerShell 窗口中操作条目。

```powershell
$api = 'http://127.0.0.1:8000'
Invoke-RestMethod "$api/health"
$item = Invoke-RestMethod "$api/api/items" -Method Post -ContentType 'application/json' -Body '{"name":"example","status":"active"}'
Invoke-RestMethod "$api/api/items?name=exam&status=active&sort=-updated_at&limit=10"
Invoke-RestMethod "$api/api/items/$($item.id)"
Invoke-RestMethod "$api/api/items/$($item.id)" -Method Put -ContentType 'application/json' -Body '{"name":"updated"}'
Invoke-RestMethod "$api/api/items/$($item.id)" -Method Delete
```

## CORS、鉴权与限流

默认允许同源浏览器请求。跨域前端需通过 `--cors-origin` 加入来源白名单。参数可以重复，值必须与浏览器 `Origin` 一致，包含协议、主机和非默认端口。预检支持路由允许的方法，以及 `Authorization` 和 `Content-Type` 请求头。来源被拒绝时返回 `403`。

在 PowerShell 中启动启用鉴权的服务：

```powershell
$env:PYTHON_BACKEND_TOKEN = py -c "import secrets; print(secrets.token_urlsafe(32))"
py .\server.py --cors-origin http://localhost:5173 --rate-limit 120
```

设置 `PYTHON_BACKEND_TOKEN` 后，调用业务接口需携带 `Authorization: Bearer <token>`。缺少或使用无效 Token 时返回 `401` 和 `WWW-Authenticate`。`/health` 和允许的 CORS 预检无需 Token。未设置环境变量时仍可按本机模式调用。

默认每个服务进程最多接受滚动 60 秒内的 120 个请求。健康检查、预检和鉴权失败共用配额。超限返回 `429` 和 `Retry-After`，单位为秒。`--rate-limit 0` 可关闭限流。

应用和 HTTP 协议错误统一使用 `{"error":"错误代码"}`。意外异常返回 `500` 和 `internal_server_error`，详细堆栈写入服务日志。`HEAD` 响应仅返回状态和响应头。

## 日志

脚本目录下的 `logs/server.log` 记录请求 IP、时间、方法、路径、状态码、耗时及服务错误。普通启动时终端同步显示日志，交互模式下请求日志写入文件。访问日志记录路径，省略查询参数和请求体。日志达到约 1 MiB 时轮转，最多保留 3 个历史文件。使用 `--log-file .\logs\my-server.log` 可指定日志文件。

## 备份与恢复

备份命令可在服务运行期间执行，使用 SQLite 的备份接口创建一致性快照。恢复命令校验 SQLite 完整性和条目表结构后，将快照写入新数据库文件。备份和恢复的目标文件都要求是新路径，其父目录需要存在且可写。

```powershell
py .\server.py --db .\items.sqlite3 --backup .\items-backup.sqlite3
py .\server.py --restore .\items-backup.sqlite3 --db .\items-restored.sqlite3
py .\server.py --db .\items-restored.sqlite3
```

此版本用于本机工具和开发。[Python `http.server` 文档](https://docs.python.org/3/library/http.server.html)
