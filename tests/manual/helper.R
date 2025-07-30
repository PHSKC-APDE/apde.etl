library(glue)

# Common test setup functions
get_test_schema <- function() {
  return(Sys.getenv("USERNAME"))
}

create_test_table <- function(conn, table_name, schema = get_test_schema()) {
  sql <- glue::glue_sql("
    CREATE TABLE {`schema`}.{`table_name`} (
      id INT,
      name VARCHAR(50),
      value DECIMAL(10,2),
      date_col DATE
    )", .con = conn)

  DBI::dbExecute(conn, sql)

  # Insert some test data
  insert_sql <- glue::glue_sql("
    INSERT INTO {`schema`}.{`table_name`} VALUES
    (1, 'Test1', 10.50, '2024-01-01'),
    (2, 'Test2', 20.75, '2024-01-02'),
    (3, 'Test3', 30.25, '2024-01-03')
  ", .con = conn)

  DBI::dbExecute(conn, insert_sql)
}

drop_test_table <- function(conn, table_name, schema = get_test_schema()) {
  sql <- glue::glue_sql("DROP TABLE IF EXISTS {`schema`}.{`table_name`}", .con = conn)
  try(DBI::dbExecute(conn, sql), silent = TRUE)
}

table_exists <- function(conn, table_name, schema = get_test_schema()) {
  result <- DBI::dbGetQuery(conn, glue::glue_sql("
    SELECT COUNT(*) as cnt
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {table_name}
  ", .con = conn))
  return(result$cnt > 0)
}
