CREATE TABLE dc_servers (
    guild_id VARCHAR(50) PRIMARY KEY,
    channel_id VARCHAR(50) NOT NULL,
    guild_name VARCHAR(255),
    updated_at DATETIME
);
