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
