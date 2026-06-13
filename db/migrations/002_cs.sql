-- 设计文档「七、核心表结构设计」客服编排相关表（7.6 / 7.7 / 7.8）。
-- 对应设计文档「六、客服系统编排」: 会话状态机 + 坐席调度 + 工单。

-- 7.6 客服会话表（状态机载体）
CREATE TABLE `t_cs_session` (
  `id`              BIGINT       NOT NULL AUTO_INCREMENT,
  `conversation_id` VARCHAR(64)  NOT NULL COMMENT '关联 t_conversation',
  `visitor_uid`     BIGINT       NOT NULL COMMENT '访客用户',
  `agent_id`        BIGINT       NULL     COMMENT '当前坐席，排队/机器人阶段为空',
  `state`           TINYINT      NOT NULL DEFAULT 0 COMMENT '0排队 1机器人 2人工服务 3已关闭',
  `priority`        TINYINT      NOT NULL DEFAULT 0 COMMENT '优先级，越大越靠前',
  `skill_group`     VARCHAR(64)  NULL     COMMENT '技能组路由标识',
  `source`          VARCHAR(64)  NULL     COMMENT '来源渠道(app/web/h5...)',
  `enqueued_at`     DATETIME     NULL     COMMENT '进入排队时间',
  `assigned_at`     DATETIME     NULL     COMMENT '分配坐席时间',
  `closed_at`       DATETIME     NULL     COMMENT '关闭时间',
  `close_reason`    TINYINT      NOT NULL DEFAULT 0 COMMENT '0未关闭 1访客主动 2坐席关闭 3超时 4系统',
  `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_conversation` (`conversation_id`),
  KEY `idx_agent_state` (`agent_id`, `state`),
  KEY `idx_state_priority` (`state`, `priority`, `enqueued_at`),
  KEY `idx_visitor` (`visitor_uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客服会话(状态机)';

-- 7.7 坐席表（调度依据）
CREATE TABLE `t_agent` (
  `agent_id`        BIGINT       NOT NULL COMMENT '坐席 uid',
  `name`            VARCHAR(128) NULL,
  `skill_group`     VARCHAR(64)  NULL     COMMENT '所属技能组',
  `max_sessions`    INT          NOT NULL DEFAULT 5 COMMENT '最大并发会话数',
  `cur_sessions`    INT          NOT NULL DEFAULT 0 COMMENT '当前进行中会话数(最小连接调度依据)',
  `status`          TINYINT      NOT NULL DEFAULT 0 COMMENT '0离线 1在线空闲 2忙碌 3小休',
  `last_active_at`  DATETIME     NULL,
  `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`agent_id`),
  KEY `idx_group_status` (`skill_group`, `status`, `cur_sessions`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='坐席表';

-- 7.8 工单表（会话沉淀）
CREATE TABLE `t_ticket` (
  `id`              BIGINT       NOT NULL AUTO_INCREMENT,
  `ticket_no`       VARCHAR(64)  NOT NULL COMMENT '工单号(对外展示)',
  `cs_session_id`   BIGINT       NULL     COMMENT '关联 t_cs_session.id',
  `visitor_uid`     BIGINT       NOT NULL,
  `agent_id`        BIGINT       NULL,
  `category`        VARCHAR(64)  NULL     COMMENT '问题分类',
  `title`           VARCHAR(256) NULL,
  `status`          TINYINT      NOT NULL DEFAULT 0 COMMENT '0待处理 1处理中 2已解决 3已关闭',
  `priority`        TINYINT      NOT NULL DEFAULT 0,
  `rating`          TINYINT      NULL     COMMENT '满意度评分 1-5',
  `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ticket_no` (`ticket_no`),
  KEY `idx_session` (`cs_session_id`),
  KEY `idx_visitor_status` (`visitor_uid`, `status`),
  KEY `idx_agent_status` (`agent_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='工单表';
