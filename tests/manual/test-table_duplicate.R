library(testthat)
library(apde.etl)
source("tests/manual/helper.R")

test_that("table_duplicate copies table structure and data", {
  # Setup - using same server for both source and destination
  conn_from <- create_db_connection("hhsaw", prod = FALSE)
  conn_to <- create_db_connection("hhsaw", prod = FALSE)
  schema <- get_test_schema()
  source_table <- "test_duplicate_source"
  dest_table <- "test_duplicate_dest"

  # Clean up any existing tables
  drop_test_table(conn_from, source_table, schema)
  drop_test_table(conn_to, dest_table, schema)

  # Create source table with data
  create_test_table(conn_from, source_table, schema)

  # Test table duplication
  expect_no_error(
    table_duplicate(conn_from = conn_from,
                    conn_to = conn_to,
                    server_to = "hhsaw_dev", # needs to be ODBC DSN
                    db_to = "hhs_analytics_workspace",
                    from_schema = schema,
                    from_table = source_table,
                    to_schema = schema,
                    to_table = dest_table,
                    confirm_tables = FALSE)
  )

  # Verify destination table was created
  expect_true(table_exists(conn_to, dest_table, schema))

  # Verify data was copied
  source_count <- DBI::dbGetQuery(conn_from, glue::glue_sql("
    SELECT COUNT(*) as cnt FROM {`schema`}.{`source_table`}
  ", .con = conn_from))

  dest_count <- DBI::dbGetQuery(conn_to, glue::glue_sql("
    SELECT COUNT(*) as cnt FROM {`schema`}.{`dest_table`}
  ", .con = conn_to))

  expect_equal(source_count$cnt, dest_count$cnt)
  expect_equal(dest_count$cnt, 3)

  # Clean up
  drop_test_table(conn_from, source_table, schema)
  drop_test_table(conn_to, dest_table, schema)
  DBI::dbDisconnect(conn_from)
  DBI::dbDisconnect(conn_to)
})

test_that("table_duplicate works with structure only", {
  # Setup
  conn_from <- create_db_connection("hhsaw", prod = FALSE)
  conn_to <- create_db_connection("hhsaw", prod = FALSE)
  schema <- get_test_schema()
  source_table <- "test_duplicate_struct_source"
  dest_table <- "test_duplicate_struct_dest"

  # Clean up any existing tables
  drop_test_table(conn_from, source_table, schema)
  drop_test_table(conn_to, dest_table, schema)

  # Create source table with data
  create_test_table(conn_from, source_table, schema)

  # Test structure-only duplication
  expect_no_error(
    table_duplicate(conn_from = conn_from,
                    conn_to = conn_to,
                    server_to = "hhsaw_dev", # must be ODBC DSN
                    db_to = "hhs_analytics_workspace",
                    from_schema = schema,
                    from_table = source_table,
                    to_schema = schema,
                    to_table = dest_table,
                    confirm_tables = FALSE,
                    table_structure_only = TRUE)
  )

  # Verify destination table was created but is empty
  expect_true(table_exists(conn_to, dest_table, schema))

  dest_count <- DBI::dbGetQuery(conn_to, glue::glue_sql("
    SELECT COUNT(*) as cnt FROM {`schema`}.{`dest_table`}
  ", .con = conn_to))
  expect_equal(dest_count$cnt, 0)

  # Verify structure matches by checking column count
  source_cols <- DBI::dbGetQuery(conn_from, glue::glue_sql("
    SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {source_table}
  ", .con = conn_from))

  dest_cols <- DBI::dbGetQuery(conn_to, glue::glue_sql("
    SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {dest_table}
  ", .con = conn_to))

  expect_equal(source_cols$cnt, dest_cols$cnt)

  # Clean up
  drop_test_table(conn_from, source_table, schema)
  drop_test_table(conn_to, dest_table, schema)
  DBI::dbDisconnect(conn_from)
  DBI::dbDisconnect(conn_to)
})

test_that("table_duplicate_delete removes tables with specified suffix", {
  # Setup
  conn <- create_db_connection("hhsaw", prod = FALSE)
  schema <- get_test_schema()
  suffix_stem <- "_test_delete"

  # Create three tables with the suffix pattern
  table1 <- paste0("table1", suffix_stem)
  table2 <- paste0("table2", suffix_stem, "_1")
  table3 <- paste0("table3", suffix_stem, "_backup")

  # Clean up any existing tables first
  drop_test_table(conn, table1, schema)
  drop_test_table(conn, table2, schema)
  drop_test_table(conn, table3, schema)

  # Create the three test tables
  create_test_table(conn, table1, schema)
  create_test_table(conn, table2, schema)
  create_test_table(conn, table3, schema)

  # Verify all three tables exist
  expect_true(table_exists(conn, table1, schema))
  expect_true(table_exists(conn, table2, schema))
  expect_true(table_exists(conn, table3, schema))

  # Mock the user confirmation to avoid interactive prompt
  # We'll use testthat's with_mocked_bindings to mock askYesNo
  with_mocked_bindings(
    askYesNo = function(...) TRUE,
    {
      # Test table deletion with suffix
      expect_no_error(
        table_duplicate_delete(conn = conn,
                               delete_table_suffix = suffix_stem)
      )
    },
    .package = "utils"
  )

  # Verify all three tables were deleted
  expect_false(table_exists(conn, table1, schema))
  expect_false(table_exists(conn, table2, schema))
  expect_false(table_exists(conn, table3, schema))

  # Clean up connection
  DBI::dbDisconnect(conn)
})

test_that("table_duplicate_delete cancels when user declines", {
  # Setup
  conn <- create_db_connection("hhsaw", prod = FALSE)
  schema <- get_test_schema()
  suffix_stem <- "_test_cancel"
  table_name <- paste0("table", suffix_stem)

  # Clean up any existing table first
  drop_test_table(conn, table_name, schema)

  # Create test table
  create_test_table(conn, table_name, schema)

  # Verify table exists
  expect_true(table_exists(conn, table_name, schema))

  # Mock user declining confirmation
  with_mocked_bindings(
    askYesNo = function(...) FALSE,
    {
      # Test that function stops when user declines
      expect_error(
        table_duplicate_delete(conn = conn,
                               delete_table_suffix = suffix_stem),
        "Table deletion cancelled"
      )
    },
    .package = "utils"
  )

  # Verify table still exists (wasn't deleted)
  expect_true(table_exists(conn, table_name, schema))

  # Clean up
  drop_test_table(conn, table_name, schema)
  DBI::dbDisconnect(conn)
})
