### ItinBuilder 软件架构设计文档

#### 1. 项目概述
ItinBuilder 是一个基于 Rust 开发的航空行程管理与构建系统。该系统旨在高效处理机场信息、航班计划（SSIM 等格式）以及行程计算。系统采用高性能的异步框架 Actix-web 和 图数据库/文档数据库 SurrealDB，确保了处理大规模航空数据的能力。

#### 2. 技术栈
- **编程语言**: Rust (Edition 2021)
- **Web 框架**: Actix-web (异步、高性能)
- **数据库**: SurrealDB (支持图关系建模)
- **序列化/反序列化**: Serde, Serde_json
- **时间处理**: Chrono, Chrono-tz
- **日志系统**: Log4rs

#### 3. 系统分层架构
项目采用了清晰的分层设计，遵循“领域驱动设计”的部分原则，将业务逻辑与基础设施分离。

- **API 层 (`src/api/`)**: 
  - 负责处理 HTTP 请求，解析输入参数（MultipartForm, Form, Json）。
  - 执行初步的输入验证。
  - 将请求转发给 Repository 或 Service 层。
- **服务层 (`src/services/`)**: 
  - 包含复杂的跨领域业务逻辑。
  - 如 `data_service` 负责解析 SSIM 标准文件并将其转换为系统可识别的航班计划。
- **领域层 (`src/domain/`)**: 
  - 系统核心业务实体（如 `Airport`, `FlightPlan`）。
  - 包含业务规则和校验逻辑（如 `AirportCode` 的格式校验）。
  - 实现领域对象之间的转换逻辑（如 `expand` 将航班计划展开为单日飞行记录）。
- **数据访问层 (`src/db/`)**:
  - **Model**: 定义数据库表/边对应的结构体（Row 对象），实现与领域对象的转换（`TryFrom`）。
  - **Repository**: 封装具体的数据库 CRUD 操作，使用 SurrealDB 的查询语言（SurQL）。
- **基础设施/公共模块**:
  - `src/structure.rs`: 定义应用全局状态（WebData）、配置结构及共享的数据类型。
  - `src/config.rs`: 处理环境配置。

#### 4. 关键模块说明
- **机场管理 (`api/airport`)**: 提供机场的增删改查 API，确保机场信息的准确性（时区、经纬度）。
- **航班计划处理 (`api/schedule`, `domain/flightplan`)**: 支持解析标准的航班编排数据，并自动将其展开为数据库中的飞行路径点（RELATE 关系）。
- **SurrealDB 集成**: 利用 SurrealDB 的图特性，将航班表示为机场节点之间的边，方便后续的行程搜索与路径规划。

#### 5. 数据流向示例 (以添加机场为例)
1. **客户端** 发送 `PUT /airport` 请求。
2. **API 层 (`add_airport`)** 接收请求，将表单数据解析为 `AirportRow`。
3. **领域校验**: 调用 `Airport::try_from(row)` 进行业务规则校验（如经纬度范围）。
4. **持久化**: 调用 `airport_repo::add_airport`。
5. **Repository 层**: 使用 `db.insert` 将数据存入 SurrealDB。
6. **返回**: API 层根据结果返回 `201 Created` 或 `400/409/500`。

#### 6. 目录结构
```text
src/
├── api/             # 接口层 (HTTP Handlers)
├── domain/          # 领域模型 (Business Logic & Entities)
├── services/        # 业务服务层 (Complex Workflows)
├── db/              # 数据持久化层
│   ├── model/       # 数据库模型
│   └── repository/  # 数据访问接口
├── structure.rs     # 核心数据结构与应用状态
└── main.rs          # 启动入口与服务配置
```
