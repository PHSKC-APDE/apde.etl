library(testthat)
library(apde.etl)
source("tests/manual/helper.R")

test_that("add_index creates and drops indices correctly", {
  # Setup
  conn <- create_db_connection("hhsaw", prod = FALSE)
  schema <- get_test_schema()
  table_name <- "test_index_table"

  # Clean up any existing table
  drop_test_table(conn, table_name, schema)

  # Create test table
  create_test_table(conn, table_name, schema)

  # Test CCS index
  expect_no_error(
    add_index(conn,
              to_schema = schema,
              to_table = table_name,
              index_type = "ccs",
              index_name = "idx_test_ccs",
              drop_index = TRUE)
  )

  # Verify CCS index was created
  index_check <- DBI::dbGetQuery(conn, glue::glue_sql("
    SELECT i.name as index_name, i.type_desc
    FROM sys.indexes i
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = {schema} AND t.name = {table_name}
      AND i.type_desc = 'CLUSTERED COLUMNSTORE'
  ", .con = conn))

  expect_equal(nrow(index_check), 1)
  expect_equal(index_check$index_name, "idx_test_ccs")

  # Test clustered index (first drop the CCS index)
  add_index(conn,
            to_schema = schema,
            to_table = table_name,
            index_type = "cl",
            index_name = "idx_test_cl",
            index_vars = c("id", "name"),
            drop_index = TRUE)

  # Verify clustered index was created
  index_check2 <- DBI::dbGetQuery(conn, glue::glue_sql("
    SELECT i.name as index_name, i.type_desc
    FROM sys.indexes i
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = {schema} AND t.name = {table_name}
      AND i.type_desc = 'CLUSTERED'
  ", .con = conn))

  expect_equal(nrow(index_check2), 1)
  expect_equal(index_check2$index_name, "idx_test_cl")

  # Clean up
  drop_test_table(conn, table_name, schema)
  DBI::dbDisconnect(conn)
})
