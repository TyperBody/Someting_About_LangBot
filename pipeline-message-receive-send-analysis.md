# LangBot 流水线消息接收与发送机制详细分析

> 本报告深入分析 LangBot 中流水线如何接收消息、如何发送回复的完整流程，包括底层实现细节。

---

## 一、消息接收完整流程

### 1.1 消息接收链路总览

```
用户发送消息
    ↓
Platform Adapter (平台适配器)
    ↓
RuntimeBot.on_friend_message / on_group_message
    ↓
Webhook Pusher (可选，可跳过流水线)
    ↓
MessageAggregator.add_message (消息聚合/防抖)
    ↓
QueryPool.add_query (请求池)
    ↓
Controller.consumer (控制器调度)
    ↓
RuntimePipeline.run (流水线执行)
```

### 1.2 各阶段详细处理

#### 1.2.1 Platform Adapter 接收消息

平台适配器是消息进入系统的第一道门。适配器负责：
- 连接外部消息平台（如 QQ、微信、Telegram 等）
- 接收原始消息事件
- 将原始事件转换为内部事件对象
- 注册事件监听器

**事件监听器注册**（`botmgr.py:286-287`）：
```python
self.adapter.register_listener(platform_events.FriendMessage, on_friend_message)
self.adapter.register_listener(platform_events.GroupMessage, on_group_message)
```

**适配器接口**（`AbstractMessagePlatformAdapter`）：
```python
class AbstractMessagePlatformAdapter:
    async def run_async(self): ...           # 运行适配器
    async def kill(self): ...                # 停止适配器
    async def reply_message(self, ...): ...  # 发送回复
    async def reply_message_chunk(self, ...): ...  # 流式回复
    async def create_message_card(self, ...): ...  # 创建消息卡片
    async def is_stream_output_supported(self) -> bool: ...  # 是否支持流式
```

#### 1.2.2 RuntimeBot 处理消息

位置：`pkg/platform/botmgr.py`

**好友消息处理**（`on_friend_message`）：
```python
async def on_friend_message(event: FriendMessage, adapter: AbstractMessagePlatformAdapter):
    # 1. 提取图片组件用于日志
    image_components = [comp for comp in event.message_chain if isinstance(comp, Image)]
    
    # 2. 记录日志
    await self.logger.info(f'{event.message_chain}', images=image_components, ...)
    
    # 3. 推送 Webhook（可跳过流水线）
    skip_pipeline = await self.ap.webhook_pusher.push_person_message(...)
    
    # 4. 如果不跳过，进入消息聚合器
    if not skip_pipeline:
        # 获取 launcher_id（可能通过适配器自定义）
        launcher_id = event.sender.id
        if hasattr(adapter, 'get_launcher_id'):
            custom_launcher_id = adapter.get_launcher_id(event)
            if custom_launcher_id:
                launcher_id = custom_launcher_id
        
        # 解析流水线 UUID
        pipeline_uuid, routed_by_rule = self.resolve_pipeline_uuid(
            'person', launcher_id, str(event.message_chain), [...]
        )
        
        # 添加到消息聚合器
        await self.ap.msg_aggregator.add_message(
            bot_uuid=self.bot_entity.uuid,
            launcher_type=LauncherTypes.PERSON,
            launcher_id=launcher_id,
            sender_id=event.sender.id,
            message_event=event,
            message_chain=event.message_chain,
            adapter=adapter,
            pipeline_uuid=pipeline_uuid,
            routed_by_rule=routed_by_rule,
        )
```

**群消息处理**（`on_group_message`）：
```python
async def on_group_message(event: GroupMessage, adapter: AbstractMessagePlatformAdapter):
    # 类似好友消息处理，但使用群ID作为launcher_id
    launcher_id = event.group.id
    # ... 其他处理类似
```

**流水线路由解析**（`resolve_pipeline_uuid`）：
```python
def resolve_pipeline_uuid(self, launcher_type, launcher_id, message_text, element_types):
    binding_type, binding_uuid = self.get_binding_info()
    
    # 如果是 workflow 绑定，返回 None
    if binding_type == 'workflow':
        return None, False
    
    # 返回 pipeline UUID
    return binding_uuid, False
```

#### 1.2.3 MessageAggregator 消息聚合

位置：`pkg/pipeline/aggregator.py`

**功能**：消息聚合/防抖，防止用户连续发送多条消息导致多次处理

**配置读取**：
```python
async def _get_aggregation_config(self, pipeline_uuid):
    pipeline = await self.ap.pipeline_mgr.get_pipeline_by_uuid(pipeline_uuid)
    config = pipeline.pipeline_entity.config or {}
    trigger_config = config.get('trigger', {})
    aggregation_config = trigger_config.get('message-aggregation', {})
    
    enabled = aggregation_config.get('enabled', False)  # 默认关闭
    delay = aggregation_config.get('delay', 1.5)  # 延迟秒数
    delay = max(1.0, min(10.0, delay))  # 限制在 1-10 秒
    
    return enabled, delay
```

**添加消息**（`add_message`）：
```python
async def add_message(self, bot_uuid, launcher_type, launcher_id, sender_id, 
                      message_event, message_chain, adapter, pipeline_uuid, routed_by_rule):
    # 1. 获取聚合配置
    enabled, delay = await self._get_aggregation_config(pipeline_uuid)
    
    # 2. 如果关闭聚合，直接添加到 QueryPool
    if not enabled:
        await self.ap.query_pool.add_query(...)
        return
    
    # 3. 生成 session_id
    session_id = f'{bot_uuid}:{launcher_type.value}:{launcher_id}'
    
    # 4. 创建 PendingMessage
    pending_msg = PendingMessage(
        bot_uuid=bot_uuid,
        launcher_type=launcher_type,
        launcher_id=launcher_id,
        sender_id=sender_id,
        message_event=message_event,
        message_chain=message_chain,
        adapter=adapter,
        pipeline_uuid=pipeline_uuid,
        routed_by_rule=routed_by_rule,
    )
    
    # 5. 添加到缓冲区
    async with self.lock:
        if session_id in self.buffers:
            buffer = self.buffers[session_id]
            # 取消现有定时器
            if buffer.timer_task and not buffer.timer_task.done():
                buffer.timer_task.cancel()
            buffer.messages.append(pending_msg)
        else:
            buffer = SessionBuffer(session_id=session_id, messages=[pending_msg])
            self.buffers[session_id] = buffer
        
        buffer.last_message_time = time.time()
        
        # 检查是否达到最大缓冲数量（10条）
        if len(buffer.messages) >= MAX_BUFFER_MESSAGES:
            force_flush = True
        else:
            # 启动延迟刷新定时器
            buffer.timer_task = asyncio.create_task(self._delayed_flush(session_id, delay))
    
    # 6. 如果达到最大数量，立即刷新
    if force_flush:
        await self._flush_buffer(session_id)
```

**消息合并**（`_merge_messages`）：
```python
def _merge_messages(self, messages: list[PendingMessage]) -> PendingMessage:
    if len(messages) == 1:
        return messages[0]
    
    base_msg = messages[0]
    
    # 构建合并后的消息链
    merged_chain = MessageChain([])
    
    for i, msg in enumerate(messages):
        if i > 0:
            # 添加换行符分隔
            merged_chain.append(Plain(text='\n'))
        
        # 复制所有消息组件
        for component in msg.message_chain:
            merged_chain.append(component)
    
    # 保持原始 message_event 不变（保留 message_id 等元数据）
    return PendingMessage(
        bot_uuid=base_msg.bot_uuid,
        launcher_type=base_msg.launcher_type,
        launcher_id=base_msg.launcher_id,
        sender_id=base_msg.sender_id,
        message_event=base_msg.message_event,  # 不变
        message_chain=merged_chain,  # 合并后的消息链
        adapter=base_msg.adapter,
        pipeline_uuid=base_msg.pipeline_uuid,
    )
```

**延迟刷新**（`_delayed_flush`）：
```python
async def _delayed_flush(self, session_id: str, delay: float):
    try:
        await asyncio.sleep(delay)  # 等待延迟
        await self._flush_buffer(session_id)  # 刷新缓冲区
    except asyncio.CancelledError:
        # 定时器被取消（有新消息到达）
        pass
```

**刷新缓冲区**（`_flush_buffer`）：
```python
async def _flush_buffer(self, session_id: str):
    async with self.lock:
        buffer = self.buffers.pop(session_id, None)
    
    if buffer is None or not buffer.messages:
        return
    
    if len(buffer.messages) == 1:
        # 只有一条消息，直接添加到 QueryPool
        msg = buffer.messages[0]
        await self.ap.query_pool.add_query(...)
        return
    
    # 合并多条消息
    merged_msg = self._merge_messages(buffer.messages)
    await self.ap.query_pool.add_query(...)
```

#### 1.2.4 QueryPool 请求池

位置：`pkg/pipeline/pool.py`

**功能**：存储待处理的请求，供控制器调度

**添加请求**（`add_query`）：
```python
async def add_query(self, bot_uuid, launcher_type, launcher_id, sender_id,
                    message_event, message_chain, adapter, pipeline_uuid, routed_by_rule):
    async with self.condition:
        # 1. 生成 query_id
        query_id = self.query_id_counter
        
        # 2. 创建 Query 对象
        query = pipeline_query.Query(
            bot_uuid=bot_uuid,
            query_id=query_id,
            launcher_type=launcher_type,
            launcher_id=launcher_id,
            sender_id=sender_id,
            message_event=message_event,
            message_chain=message_chain,
            variables={'_routed_by_rule': routed_by_rule},  # 初始变量
            resp_messages=[],  # 空列表
            resp_message_chain=[],  # 空列表
            adapter=adapter,
            pipeline_uuid=pipeline_uuid,
        )
        
        # 3. 添加到请求池
        self.queries.append(query)
        self.cached_queries[query_id] = query  # 缓存，用于插件反向调用
        
        # 4. 计数器自增
        self.query_id_counter += 1
        
        # 5. 通知控制器有新请求
        self.condition.notify_all()
```

#### 1.2.5 Controller 控制器调度

位置：`pkg/pipeline/controller.py`

**功能**：从请求池获取请求，调度到对应的流水线执行

**消费者循环**（`consumer`）：
```python
async def consumer(self):
    while True:
        selected_query = None
        
        # 1. 从请求池获取请求
        async with self.ap.query_pool:
            queries = self.ap.query_pool.queries
            
            for query in queries:
                session = await self.ap.sess_mgr.get_session(query)
                
                # 检查会话是否达到并发上限
                if not session._semaphore.locked():
                    selected_query = query
                    await session._semaphore.acquire()  # 获取信号量
                    break
            
            if selected_query:
                queries.remove(selected_query)  # 从池中移除
            else:
                # 没有请求或都达到上限，等待
                await self.ap.query_pool.condition.wait()
                continue
        
        # 2. 创建任务处理请求
        if selected_query:
            async def _process_query(query):
                async with self.semaphore:  # 总并发上限
                    # 查找流水线
                    pipeline_uuid = query.pipeline_uuid
                    if pipeline_uuid:
                        pipeline = await self.ap.pipeline_mgr.get_pipeline_by_uuid(pipeline_uuid)
                        if pipeline:
                            await pipeline.run(query)  # 执行流水线
                        else:
                            self.ap.logger.warning(f'Pipeline {pipeline_uuid} not found')
                    else:
                        self.ap.logger.warning(f'No pipeline_uuid for query')
                
                # 释放会话信号量
                async with self.ap.query_pool:
                    session = await self.ap.sess_mgr.get_session(query)
                    session._semaphore.release()
                    # 通知其他协程
                    self.ap.query_pool.condition.notify_all()
            
            # 创建任务
            self.ap.task_mgr.create_task(
                _process_query(selected_query),
                kind='query',
                name=f'query-{selected_query.query_id}',
                scopes=[...],
            )
```

---

## 二、消息发送完整流程

### 2.1 消息发送链路总览

```
Stage 执行链处理
    ↓
MessageProcessor 生成 resp_messages
    ↓
ResponseWrapper 转换为 resp_message_chain
    ↓
_check_output 检查输出
    ↓
Adapter.reply_message / reply_message_chunk
    ↓
Platform Adapter 发送到平台
    ↓
用户收到回复
```

### 2.2 各阶段详细处理

#### 2.2.1 MessageProcessor 生成回复

位置：`pkg/pipeline/process/handlers/chat.py`

**读取字段**：
- `query.message_chain` - 用户消息
- `query.prompt` - 情景预设
- `query.messages` - 历史消息
- `query.user_message` - 用户消息对象
- `query.use_llm_model_uuid` - 模型 UUID
- `query.use_funcs` - 函数工具
- `query.pipeline_config` - 流水线配置
- `query.adapter` - 适配器（检查流式支持）

**写入字段**：
- `query.resp_messages` - 回复消息列表
- `query.session.using_conversation.messages` - 保存对话历史

**流式输出处理**：
```python
async def handle(self, query):
    # 1. 触发 NormalMessageReceived 事件
    event = PersonNormalMessageReceived|GroupNormalMessageReceived(...)
    event_ctx = await self.ap.plugin_connector.emit_event(event, bound_plugins)
    
    # 2. 检查事件是否阻止默认行为
    if event_ctx.is_prevented_default():
        if event_ctx.event.reply_message_chain:
            query.resp_messages.append(event_ctx.event.reply_message_chain)
            yield StageProcessResult(CONTINUE, query)
        else:
            yield StageProcessResult(INTERRUPT, query)
    else:
        # 3. 检查是否修改用户消息
        if event_ctx.event.user_message_alter:
            query.user_message.content = event_ctx.event.user_message_alter
        
        # 4. 检查是否支持流式输出
        is_stream = await query.adapter.is_stream_output_supported()
        
        # 5. 获取 Runner
        runner = runner_module.preregistered_runners[...](self.ap, query.pipeline_config)
        
        # 6. 执行 Runner
        if is_stream:
            resp_message_id = uuid.uuid4()
            
            async for result in runner.run(query):
                result.resp_message_id = str(resp_message_id)
                
                # 移除旧结果，添加新结果
                if query.resp_messages:
                    query.resp_messages.pop()
                if query.resp_message_chain:
                    query.resp_message_chain.pop()
                
                # 创建消息卡片（第一次）
                if not is_create_card:
                    await query.adapter.create_message_card(str(resp_message_id), query.message_event)
                    is_create_card = True
                
                query.resp_messages.append(result)
                
                # 每个 chunk 都 yield，触发后续 Stage
                yield StageProcessResult(CONTINUE, query)
        else:
            async for result in runner.run(query):
                query.resp_messages.append(result)
                yield StageProcessResult(CONTINUE, query)
        
        # 7. 保存对话历史
        query.session.using_conversation.messages.append(query.user_message)
        query.session.using_conversation.messages.extend(query.resp_messages)
```

#### 2.2.2 ResponseWrapper 包装回复

位置：`pkg/pipeline/wrapper/wrapper.py`

**功能**：将 `resp_messages` 转换为 `resp_message_chain`

```python
async def process(self, query, stage_inst_name):
    # 1. 如果已经是 MessageChain，直接使用
    if isinstance(query.resp_messages[-1], MessageChain):
        query.resp_message_chain.append(query.resp_messages[-1])
        yield StageProcessResult(CONTINUE, query)
    else:
        # 2. 根据角色类型处理
        if query.resp_messages[-1].role == 'command':
            query.resp_message_chain.append(
                query.resp_messages[-1].get_content_platform_message_chain(prefix_text='[bot] ')
            )
            yield StageProcessResult(CONTINUE, query)
        
        elif query.resp_messages[-1].role == 'plugin':
            query.resp_message_chain.append(
                query.resp_messages[-1].get_content_platform_message_chain()
            )
            yield StageProcessResult(CONTINUE, query)
        
        elif query.resp_messages[-1].role == 'assistant':
            result = query.resp_messages[-1]
            
            # 3. 触发 NormalMessageResponded 事件
            event = NormalMessageResponded(
                response_text=reply_text,
                funcs_called=[...],
                query=query,
            )
            event_ctx = await self.ap.plugin_connector.emit_event(event, bound_plugins)
            
            # 4. 检查事件是否阻止默认行为
            if event_ctx.is_prevented_default():
                yield StageProcessResult(INTERRUPT, query)
            else:
                # 5. 使用事件修改的回复或原始回复
                if event_ctx.event.reply_message_chain:
                    query.resp_message_chain.append(event_ctx.event.reply_message_chain)
                else:
                    query.resp_message_chain.append(result.get_content_platform_message_chain())
                
                yield StageProcessResult(CONTINUE, query)
```

#### 2.2.3 _check_output 发送回复

位置：`pkg/pipeline/pipelinemgr.py:140`

**功能**：检查 Stage 返回结果中的通知字段，并发送到平台

```python
async def _check_output(self, query, result):
    # 1. 检查是否有用户通知
    if result.user_notice:
        # 2. 处理字符串类型
        if isinstance(result.user_notice, str):
            result.user_notice = MessageChain([Plain(text=result.user_notice)])
        elif isinstance(result.user_notice, list):
            result.user_notice = MessageChain(*result.user_notice)
        
        # 3. @发送者（群聊）
        if query.pipeline_config['output']['misc']['at-sender'] and \
           isinstance(query.message_event, GroupMessage):
            result.user_notice.insert(0, At(target=query.message_event.sender.id))
        
        # 4. 根据是否支持流式输出选择发送方式
        if await query.adapter.is_stream_output_supported() and query.resp_messages:
            # 流式输出
            await query.adapter.reply_message_chunk(
                message_source=query.message_event,
                bot_message=query.resp_messages[-1],
                message=result.user_notice,
                quote_origin=query.pipeline_config['output']['misc']['quote-origin'],
                is_final=[msg.is_final for msg in query.resp_messages][0],
            )
        else:
            # 普通回复
            await query.adapter.reply_message(
                message_source=query.message_event,
                message=result.user_notice,
                quote_origin=query.pipeline_config['output']['misc']['quote-origin'],
            )
    
    # 5. 处理其他通知
    if result.debug_notice:
        self.ap.logger.debug(result.debug_notice)
    if result.console_notice:
        self.ap.logger.info(result.console_notice)
    if result.error_notice:
        self.ap.logger.error(result.error_notice)
        # 标记错误
        query.variables['_monitoring_has_error'] = True
```

#### 2.2.4 Adapter 发送消息

适配器接口定义在 `langbot_plugin/api/definition/abstract/platform/adapter.py`

**发送回复**（`reply_message`）：
```python
async def reply_message(self, message_source, message, quote_origin):
    # 实现由具体适配器提供
    # 如 QQ 适配器会调用 QQ API 发送消息
    pass
```

**流式回复**（`reply_message_chunk`）：
```python
async def reply_message_chunk(self, message_source, bot_message, message, quote_origin, is_final):
    # 实现由具体适配器提供
    # 如 QQ 适配器会调用 QQ API 发送流式消息
    pass
```

**创建消息卡片**（`create_message_card`）：
```python
async def create_message_card(self, message_id, message_source):
    # 实现由具体适配器提供
    # 用于流式输出时创建卡片
    pass
```

---

## 三、消息接收与发送的字段流转

### 3.1 消息接收字段流转

```mermaid
flowchart LR
    A[用户消息] --> B[Platform Adapter]
    B --> |FriendMessage/GroupMessage| C[RuntimeBot]
    C --> |message_event, message_chain| D[MessageAggregator]
    D --> |合并后的 message_chain| E[QueryPool]
    E --> |Query 对象| F[Controller]
    F --> |Query 对象| G[RuntimePipeline]
```

### 3.2 消息发送字段流转

```mermaid
flowchart LR
    A[Runner] --> |Message 对象| B[resp_messages]
    B --> |Message 对象| C[ResponseWrapper]
    C --> |MessageChain| D[resp_message_chain]
    D --> |MessageChain| E[_check_output]
    E --> |MessageChain| F[Adapter]
    F --> |API 调用| G[平台发送消息]
```

### 3.3 Query 对象字段完整生命周期

| 阶段 | 字段 | 来源/设置者 | 用途 |
|------|------|-------------|------|
| 接收 | `message_event` | Platform Adapter | 原始事件对象 |
| 接收 | `message_chain` | Platform Adapter | 原始消息链 |
| 接收 | `launcher_type` | RuntimeBot | 会话类型(person/group) |
| 接收 | `launcher_id` | RuntimeBot | 会话ID |
| 接收 | `sender_id` | RuntimeBot | 发送者ID |
| 接收 | `adapter` | RuntimeBot | 消息平台适配器 |
| 接收 | `bot_uuid` | RuntimeBot | 机器人UUID |
| 接收 | `pipeline_uuid` | RuntimeBot | 流水线UUID |
| 接收 | `variables['_routed_by_rule']` | QueryPool | 路由规则标记 |
| 预处理 | `pipeline_config` | RuntimePipeline.run | 流水线配置 |
| 预处理 | `session` | PreProcessor | 会话对象 |
| 预处理 | `messages` | PreProcessor | 历史消息列表 |
| 预处理 | `prompt` | PreProcessor | 情景预设 |
| 预处理 | `user_message` | PreProcessor | 用户消息对象 |
| 预处理 | `use_llm_model_uuid` | PreProcessor | 模型UUID |
| 预处理 | `use_funcs` | PreProcessor | 函数工具列表 |
| 预处理 | `variables[...]` | PreProcessor | 各种变量 |
| 处理 | `resp_messages` | MessageProcessor | 回复消息列表 |
| 包装 | `resp_message_chain` | ResponseWrapper | 回复消息链 |
| 发送 | `current_stage_name` | RuntimePipeline | 当前阶段名称 |

---

## 四、流式输出特殊处理

### 4.1 流式输出链路

```
Runner 流式输出
    ↓
每个 chunk 更新 resp_messages
    ↓
yield StageProcessResult(CONTINUE, query)
    ↓
_execute_from_stage 递归执行后续 Stage
    ↓
ResponseWrapper 处理每个 chunk
    ↓
_check_output 发送每个 chunk
    ↓
Adapter.reply_message_chunk 发送流式片段
```

### 4.2 流式输出关键代码

**MessageProcessor 流式处理**：
```python
if is_stream:
    resp_message_id = uuid.uuid4()
    
    async for result in runner.run(query):
        result.resp_message_id = str(resp_message_id)
        
        # 移除旧结果，添加新结果
        if query.resp_messages:
            query.resp_messages.pop()
        query.resp_messages.append(result)
        
        # 每个 chunk 都 yield，触发后续 Stage
        yield StageProcessResult(CONTINUE, query)
```

**_execute_from_stage 递归执行**：
```python
elif isinstance(result, typing.AsyncGenerator):
    async for sub_result in result:
        await self._check_output(query, sub_result)
        
        if sub_result.result_type == CONTINUE:
            query = sub_result.new_query
            # 递归执行后续 Stage
            await self._execute_from_stage(i + 1, query)
    break
```

### 4.3 流式输出配置

**检查流式支持**：
```python
is_stream = await query.adapter.is_stream_output_supported()
```

**适配器流式发送**：
```python
await query.adapter.reply_message_chunk(
    message_source=query.message_event,
    bot_message=query.resp_messages[-1],
    message=result.user_notice,
    quote_origin=query.pipeline_config['output']['misc']['quote-origin'],
    is_final=[msg.is_final for msg in query.resp_messages][0],
)
```

---

## 五、WebSocket 特殊处理

### 5.1 WebSocket 适配器

位置：`pkg/platform/sources/websocket_adapter.py`

**特殊字段**：
```python
message_lists: dict[str, list[WebSocketMessage]]       # {pipeline_uuid: [messages]}
stream_message_indexes: dict[str, dict[str, int]]      # {pipeline_uuid: {resp_message_id: index}}
```

**消息广播**：
```python
await ws_connection_manager.broadcast_to_pipeline(
    pipeline_uuid,
    {
        'type': 'message',
        'data': message_data,
        'session_type': session_type,
    }
)
```

### 5.2 WebSocket 消息接收

WebSocket 适配器接收消息后，同样经过：
1. 转换为 FriendMessage/GroupMessage 事件
2. 注册到 RuntimeBot 的监听器
3. 进入 MessageAggregator
4. 进入 QueryPool
5. 由 Controller 调度执行

---

## 六、Webhook 跳过机制

### 6.1 Webhook 推送

位置：`pkg/platform/botmgr.py`

**好友消息 Webhook**：
```python
skip_pipeline = await self.ap.webhook_pusher.push_person_message(
    event, self.bot_entity.uuid, adapter.__class__.__name__
)

if skip_pipeline:
    await self.logger.info('Pipeline skipped for person message due to webhook response')
    return
```

**群消息 Webhook**：
```python
skip_pipeline = await self.ap.webhook_pusher.push_group_message(
    event, self.bot_entity.uuid, adapter.__class__.__name__
)

if skip_pipeline:
    await self.logger.info('Pipeline skipped for group message due to webhook response')
    return
```

### 6.2 丢弃消息记录

当消息被路由规则丢弃时，会记录到监控系统：
```python
async def _record_discarded_message(self, launcher_type, launcher_id, sender_id, 
                                     message_event, message_chain):
    await self.ap.monitoring_service.record_message(
        bot_id=self.bot_entity.uuid,
        bot_name=self.bot_entity.name or self.bot_entity.uuid,
        pipeline_id=self.PIPELINE_DISCARD,
        pipeline_name=self.PIPELINE_DISCARD_DISPLAY_NAME,
        message_content=message_content,
        session_id=session_id,
        status='discarded',
        level='info',
        ...
    )
```

---

## 七、并发控制

### 7.1 信号量控制

**Controller 总并发**：
```python
self.semaphore = asyncio.Semaphore(self.ap.instance_config.data['concurrency']['pipeline'])
```

**Session 会话并发**：
```python
session = await self.ap.sess_mgr.get_session(query)
if not session._semaphore.locked():
    selected_query = query
    await session._semaphore.acquire()
```

### 7.2 并发控制流程

```
QueryPool 有请求
    ↓
Controller 检查 Session 信号量
    ↓
未锁定 → 获取信号量 → 从池中移除 → 创建任务
    ↓
任务获取 Controller 信号量
    ↓
执行流水线
    ↓
释放 Session 信号量
    ↓
通知 QueryPool 有新请求
```

---

## 八、错误处理

### 8.1 流水线执行错误

位置：`pkg/pipeline/pipelinemgr.py`

```python
async def process_query(self, query):
    try:
        await self._execute_from_stage(0, query)
        
        # 记录成功
        if not query.variables.get('_monitoring_has_error', False):
            await monitoring_helper.MonitoringHelper.record_query_success(...)
            await monitoring_helper.MonitoringHelper.record_query_response(...)
    
    except Exception as e:
        inst_name = query.current_stage_name or 'unknown'
        self.ap.logger.error(f'Error processing query {query.query_id} stage={inst_name}: {e}')
        
        # 记录错误
        await monitoring_helper.MonitoringHelper.record_query_error(...)
    
    finally:
        # 清理缓存
        del self.ap.query_pool.cached_queries[query.query_id]
```

### 8.2 Stage 错误处理

Stage 可以通过 `StageProcessResult` 返回错误通知：
```python
yield StageProcessResult(
    result_type=INTERRUPT,
    new_query=query,
    user_notice='错误提示',  # 发送给用户
    error_notice='错误详情',  # 记录到日志
    debug_notice='调试信息',  # 调试输出
)
```

### 8.3 Runner 执行错误

位置：`pkg/pipeline/process/handlers/chat.py`

```python
except Exception as e:
    error_info = f'{traceback.format_exc()}'
    self.ap.logger.error(f'Conversation({query.query_id}) Request Failed: {error_info}')
    
    exception_handling = query.pipeline_config['output']['misc'].get('exception-handling', 'show-hint')
    
    if exception_handling == 'show-error':
        user_notice = f'{e}'
    elif exception_handling == 'show-hint':
        user_notice = query.pipeline_config['output']['misc'].get('failure-hint', 'Request failed.')
    else:  # hide
        user_notice = None
    
    yield StageProcessResult(
        result_type=INTERRUPT,
        new_query=query,
        user_notice=user_notice,
        error_notice=f'{e}',
        debug_notice=traceback.format_exc(),
    )
```

---

## 九、消息类型处理

### 9.1 接收的消息类型

Platform Adapter 接收的消息会转换为内部的 `MessageChain` 对象，支持：
- `Plain` - 纯文本
- `Image` - 图片
- `Voice` - 语音
- `File` - 文件
- `Quote` - 引用消息
- `At` - @提及
- 等其他类型

### 9.2 发送的消息类型

ResponseWrapper 会将回复转换为 `MessageChain` 发送，支持：
- `Plain` - 纯文本
- `Image` - 图片
- `At` - @提及
- `Forward` - 转发消息（长文本处理）
- 等其他类型

---

*报告生成时间: 2026-05-20*
*分析基于代码版本: LangBot_copy*
