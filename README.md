# 大数据系统基本实验

本仓库为“大数据管理与分析”实验一代码，包含 HDFS Java API 综合操作和三道 MapReduce 程序。

## 实验环境

- Ubuntu 22.04
- OpenJDK 8
- Apache Hadoop 3.2.2
- HDFS 伪分布式模式

## 主要内容

- `HdfsOperations.java`：上传、下载、查看、递归列表、创建、删除、追加和移动等 HDFS Java API 操作。
- `MergeDedup.java`：合并多个输入文件并去除重复记录。
- `IntegerSort.java`：整数全局排序并生成位次。
- `Grandparent.java`：根据父子关系挖掘祖孙关系。
- `04_测试数据`：三道 MapReduce 程序的输入数据及预期结果。
- `05_运行脚本`：环境检查、编译、上传输入、运行作业和查看结果脚本。

## 运行顺序

在 Hadoop 已启动的 Ubuntu 环境中执行：

```bash
bash 05_运行脚本/check_environment.sh
bash 05_运行脚本/build.sh
bash 05_运行脚本/prepare_inputs.sh
bash 05_运行脚本/run_mapreduce.sh
bash 05_运行脚本/show_results.sh
```

MapReduce 输出目录为：

```text
/user/vboxuser/experiment1/output
```

## 实际运行结果

- 合并去重、整数排序、祖孙关系挖掘三道作业均成功完成。
- YARN 中三道作业状态均为 `FINISHED`，最终状态均为 `SUCCEEDED`。
- 三道程序的实际输出均与测试数据中的预期结果一致。
