# iptv-api-plugin

![Dispatcharr plugin](https://img.shields.io/badge/Dispatcharr-plugin-8A2BE2)
![Version](https://img.shields.io/badge/version-1.0.5-blue)
![License](https://img.shields.io/badge/license-MIT-green)

一个适用于 `Dispatcharr` 的插件，用于处理 `iptv-api` 生成的流，按名称归并频道，清理失效流和孤儿流，并支持定时运行、预览和指定频道组名。

## 功能

- 按名称归并流到同一个频道
- 支持模糊匹配，自动忽略常见符号和质量后缀
- 保留流的原始顺序
- 支持预览模式
- 清理失效流
- 清理孤儿流
- 支持定时运行
- 支持指定频道组名

## 动作

- `Run Now`：执行归并和清理
- `Preview`：只预览，不写库
- `Cleanup Now`：只执行清理
- `Sync Schedule`：同步定时任务

## 配置

- `Channel Group`：创建频道时使用的频道组名
- `Max Streams Per Channel`：每个频道保留的流数量
- `Cleanup Stale Streams`：是否删除失效流
- `Cleanup Orphan Streams`：是否删除孤儿流
- `Schedule Times`：定时运行时间，格式 `HHMM`，可逗号分隔
- `Dry Run Mode`：是否默认使用预览模式

## TODO

- `Channel Profile`：后续版本计划支持将频道加入指定 profile

## 说明

这个插件会把 `iptv-api` 生成的同名或近似名称流归并到同一个频道，并按源列表顺序保留前几个可用流。定时任务通过 Celery Beat 运行，首次需要手动点一次 `Sync Schedule` 才会创建对应计划。
