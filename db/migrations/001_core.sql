-- 设计文档「七、核心表结构设计」对应的 DDL。
-- 生产环境: t_message / t_user_inbox 按 conversation_id / uid 哈希分库分表；
-- 冷数据可下沉 HBase（rowkey = conversation_id + reverse(conv_seq)）。

-- 7.1 消息表（核心）
CREATE TABLE `t_message` (
  `id`              BIGINT       NOT NULL AUTO_INCREMENT,
  `server_msg_id`   BIGINT       NOT NULL COMMENT 'Snowflake 全局唯一 ID',
  `client_msg_id`   VARCHAR(64)  NOT NULL COMMENT '客户端 UUID，幂等去重',
  `conversation_id` VARCHAR(64)  NOT NULL,
  `conv_seq`        BIGINT       NOT NULL COMMENT '会话内严格递增序号',
  `from_uid`        BIGINT       NOT NULL,
  `conv_type`       TINYINT      NOT NULL COMMENT '1单聊 2群聊 3客服',
  `sender_role`     TINYINT      NOT NULL DEFAULT 1 COMMENT '1用户 2坐席 3机器人 4系统',
  `msg_type`        TINYINT      NOT NULL COMMENT '1文本 2图片 3自定义',
  `content`         MEDIUMBLOB   NULL COMMENT '消息体(protobuf/json)',
  `send_time_ms`    BIGINT       NOT NULL,
  `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_conv_seq` (`conversation_id`, `conv_seq`),
  UNIQUE KEY `uk_client_msg` (`conversation_id`, `client_msg_id`),
  KEY `idx_server_msg` (`server_msg_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='消息表';

-- 7.2 会话表
CREATE TABLE `t_conversation` (
  `conversation_id` VARCHAR(64)  NOT NULL,
  `conv_type`       TINYINT      NOT NULL,
  `name`            VARCHAR(128) NULL,
  `member_count`    INT          NOT NULL DEFAULT 0,
  `max_seq`         BIGINT       NOT NULL DEFAULT 0 COMMENT '当前最大 conv_seq',
  `last_msg_id`     BIGINT       NULL,
  `created_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`conversation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='会话表';

-- 7.3 会话成员表
CREATE TABLE `t_conversation_member` (
  `conversation_id` VARCHAR(64) NOT NULL,
  `uid`             BIGINT      NOT NULL,
  `role`            TINYINT     NOT NULL DEFAULT 0 COMMENT '0普通 1管理员 2群主',
  `read_seq`        BIGINT      NOT NULL DEFAULT 0 COMMENT '已读位点',
  `joined_at`       DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`conversation_id`, `uid`),
  KEY `idx_uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='会话成员表';

-- 7.4 用户收件箱表（写扩散）
CREATE TABLE `t_user_inbox` (
  `uid`             BIGINT      NOT NULL,
  `inbox_seq`       BIGINT      NOT NULL COMMENT '用户维度递增序号',
  `conversation_id` VARCHAR(64) NOT NULL,
  `conv_seq`        BIGINT      NOT NULL,
  `server_msg_id`   BIGINT      NOT NULL,
  `created_at`      DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`uid`, `inbox_seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户收件箱(写扩散)';

-- 7.5 离线推送/补洞辅助表
CREATE TABLE `t_offline_push` (
  `id`            BIGINT      NOT NULL AUTO_INCREMENT,
  `uid`           BIGINT      NOT NULL,
  `server_msg_id` BIGINT      NOT NULL,
  `status`        TINYINT     NOT NULL DEFAULT 0 COMMENT '0待推 1已推 2失败',
  `retry_count`   INT         NOT NULL DEFAULT 0,
  `created_at`    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_uid_status` (`uid`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='离线推送队列';
