# HBasePicWrite
##项目理解：

## 流程图

	App启动 ==》检查环境==>Consumer 多线程写入（后台） ==》检查历史（就是看看有哪些写入失败或新写入的文件）-
						
										
## 模块
	App[项目启动类]
	Consumer[消费类主要写入]
	FileMonitor[文件监控类 当有新文件进行缓存]
	HBaseWrapper[HBase 封装类]
	Producer[生产类]
	ServiceConfig[配置读取类]
