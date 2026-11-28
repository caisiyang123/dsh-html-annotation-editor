# HTML 批注编辑器 / HTML Annotation Editor

在 DeepSeek Harness / BISHENG 的右侧预览 HTML：鼠标预高亮、点击锁定位置、输入修改意见，再交给当前会话的 Agent 编辑源文件。支持批注草稿、折叠意见面板和按回合完成证据推进的串行任务队列。

Preview local HTML in the right sidebar, annotate page elements, and send editing requests to the current Agent session with a persistent serial queue.

**当前版本：0.2.0 · MIT License · 首个公开提交版。**

[完整中文使用说明书](USER_GUIDE.md) · [版本记录](CHANGELOG.md) · [反馈问题](https://github.com/caisiyang123/dsh-html-annotation-editor/issues)

## 功能

- 虚线悬停预高亮、实线点击选区，两者独立；选区不随鼠标漂移。
- 对元素或整页提出意见，多条批注可以组成一项 Agent 任务。
- 按会话绑定文件与任务，上一项对应回合完成后才推进下一项。
- 支持排队状态、取消未发送任务、失败重试与不确定状态暂停。
- 下方“修改意见”标题栏直接收起与展开；浏览模式腾出预览空间。
- 预览宽度可切换，宿主分隔条补充键盘、恢复与偏好持久化。
- 可信本地密码页可在开启脚本后使用；不绕过密码校验。
- 嵌套 iframe 按整个区域定位，浏览模式恢复内部交互。

## 安装

当前版本在基于 DSH **0.2.1-rc.2** 的 BISHENG macOS 客户端验证。声明支持的 DSH 范围为 `>=0.2.1-rc.2 <0.3.0`，其他版本/系统尚未完整验收。

社区目录收录申请与上架是独立过程；目录未合并前不能声称已经在市场可搜索。

普通 dsh web：

```sh
dsh plugin --profile web add https://github.com/caisiyang123/dsh-html-annotation-editor/releases/download/v0.2.0/dsh-html-annotation-editor-0.2.0.tgz
```

桌面端请从应用自己的插件管理入口安装。部分官方桌面版本仅接受 npm 来源，可能不支持 GitHub tarball；请不要为安装而修改应用安装包。当前发布只提供 GitHub 源码与 Release 包，尚未发布 npm。

安装后刷新 GUI。根据宿主提示决定是否还需要重新加载插件或重启。

## 快速开始

1. 点击“HTML 批注”，打开工作区相对路径或宿主允许读取的完整 HTML 路径。
2. 打开即进入“浏览页面”，默认信任并运行页面脚本；交互页面可直接使用，需要静态查看时取消“信任脚本”。
3. 选择“点选批注”，鼠标划过目标，点击后在下方填写意见。
4. 点击“添加批注”继续积累，或“发送给 Agent”启动当前任务。Ctrl/⌘ + Enter 也可发送。
5. 后续任务依次等待。结合原生对话检查执行结果，使用文档刷新查看源文件修改。

## 信任、数据与边界

- “信任脚本”默认开启，可随时取消。开启后，脚本可以读取打包的文档资源并访问其允许的 HTTPS 来源；取消后改用仅允许注入桥接脚本的 nonce 策略，清除原页面脚本、内联事件与活动 URI。只对可信来源的 HTML 保持开启。
- 本地资源限制为文档目录内的常见静态扩展名；拒绝直接上级目录、绝对路径和非常规文件类型。CSS import 最多 4 层，最多 96 次加载，单项 8 MiB，总量 40 MiB。
- 独立 iframe 保留沙箱，不授予 `allow-same-origin`；浏览模式允许本地表单 JS 校验，CSP 仍禁止真正向外提交表单。远程登录、复杂 module、动态资源与部分 srcset 不保证支持。
- 点发送后，意见、文件路径、元素摘录与有限 HTML 片段进入当前 Agent 对话；模型费用和数据处理规则沿用宿主。插件没有自建云端接口，文档外部资源可能请求其自身来源。
- 草稿和完整任务提示词以明文保存在本机 localStorage，不跨设备同步、不加密、暂不自动清理。历史配额耗尽时写入失败会阻止发送。不要在批注中填写密码或密钥。
- 未提交任务需要 GUI 页面打开才能推进。刷新后如宿主事件历史不完整，任务可能安全地等待而不是自动恢复；先核对原生对话，不要重复发送。
- 全部未完成/失败条目与最近 10 项完成/取消条目可见。已被宿主接纳的任务请用原生对话停止功能处理。
- iframe 仅支持区域级定位。此插件与其他 HTML 渲染器替换插件可能冲突。

## 开发

无第三方运行时代码依赖需要另外安装；客户端使用宿主提供的 React、slots、workspaceFiles 和会话控制器。

```sh
node build.mjs
node --test queue-core.test.mjs preview-core.test.mjs
```

当前自动化测试覆盖队列与桥接行为。实际 GUI 已验证拖动、折叠、草稿保留、串行模拟任务、本地密码表单与嵌套区域高亮；未把小样本模拟验收等同于所有宿主/文件兼容保证。

目录中的 `demo/` 只包含公开演示素材；本机配置备份、业务文件和私人测试记录不属于发行内容。
