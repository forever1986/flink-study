CREATE TABLE flink_test.log_table (
	server_id varchar(100) NOT NULL,
	cpu_value DOUBLE NULL,
	collect_time BIGINT NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;
