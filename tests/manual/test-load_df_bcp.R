library(testthat)
library(apde.etl)
source("tests/manual/helper.R")

# NOTE!!! To run the test for the Azure servers, you will need to install and
# set up 'ODBC Data Source Administrator (64-Bit). You will need to create a
# System DSN called hhsaw_dev, which connects to Azure 20.
# Be sure to use ODBC Driver 17 for SQL Server

test_that("load_df_bcp loads data correctly to on-premises servers ", {
  # Setup
  schema <- "APDE_WIP"
  table_name <- "test_bcp_load"

  # Create test data
  test_data <- data.frame(
    id = 1:5,
    name = c("Alice", "Bob", "Charlie", "David", "Eve"),
    value = c(10.5, 20.75, 30.25, 40.0, 50.5),
    date_col = as.Date(c("2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05")),
    stringsAsFactors = FALSE
  )

  # Create empty table first
  conn <- create_db_connection("phextractstore", prod = FALSE)
  drop_test_table(conn, table_name, schema)

  sql <- glue::glue_sql("
    CREATE TABLE {`schema`}.{`table_name`} (
      id INT,
      name VARCHAR(50),
      value DECIMAL(10,2),
      date_col DATE
    )", .con = conn)

  DBI::dbExecute(conn, sql)

  # Test BCP load
  expect_no_error(
    load_df_bcp(dataset = test_data,
                server = "KCITSQLUATHIP40",
                db_name = "phextractstore",
                schema_name = schema,
                table_name = table_name
                )
  )

  # Verify data was loaded
  conn <- create_db_connection("phextractstore", prod = FALSE)
  result <- DBI::dbGetQuery(conn, glue::glue_sql("
    SELECT COUNT(*) as cnt FROM {`schema`}.{`table_name`}
  ", .con = conn))
  expect_equal(result$cnt, 5)

  # Verify data content
  loaded_data <- DBI::dbGetQuery(conn, glue::glue_sql("
    SELECT * FROM {`schema`}.{`table_name`} ORDER BY id
  ", .con = conn))
  expect_equal(nrow(loaded_data), 5)
  expect_equal(loaded_data$name, test_data$name)

  # Clean up
  drop_test_table(conn, table_name, schema)
  DBI::dbDisconnect(conn)
})

test_that("load_df_bcp loads data correctly to Azure servers ", {
  # Setup
  schema <- get_test_schema()
  table_name <- "test_bcp_load"

  # Create test data
  test_data <- data.frame(
    id = 1:5,
    name = c("Alice", "Bob", "Charlie", "David", "Eve"),
    value = c(10.5, 20.75, 30.25, 40.0, 50.5),
    date_col = as.Date(c("2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05")),
    stringsAsFactors = FALSE
  )

  # Create empty table first
  conn <- create_db_connection("hhsaw", prod = FALSE)
  drop_test_table(conn, table_name, schema)

  sql <- glue::glue_sql("
    CREATE TABLE {`schema`}.{`table_name`} (
      id INT,
      name VARCHAR(50),
      value DECIMAL(10,2),
      date_col DATE
    )", .con = conn)

  DBI::dbExecute(conn, sql)

  # Test BCP load
  expect_no_error(
    load_df_bcp(dataset = test_data,
                server = "hhsaw_dev",
                db_name = "hhs_analytics_workspace",
                schema_name = schema,
                table_name = table_name,
                user = keyring::key_list("hhsaw_dev")[["username"]],
                pass = keyring::key_get("hhsaw_dev", keyring::key_list("hhsaw_dev")[["username"]])
    )
  )

  # Verify data was loaded
  conn <- create_db_connection("hhsaw", prod = FALSE)
  result <- DBI::dbGetQuery(conn, glue::glue_sql("
    SELECT COUNT(*) as cnt FROM {`schema`}.{`table_name`}
  ", .con = conn))
  expect_equal(result$cnt, 5)

  # Verify data content
  loaded_data <- DBI::dbGetQuery(conn, glue::glue_sql("
    SELECT * FROM {`schema`}.{`table_name`} ORDER BY id
  ", .con = conn))
  expect_equal(nrow(loaded_data), 5)
  expect_equal(loaded_data$name, test_data$name)

  # Clean up
  drop_test_table(conn, table_name, schema)
  DBI::dbDisconnect(conn)
})
