test_that("create_db_connection validates arguments correctly", {
  # Test for invalid server
  expect_error(create_db_connection(server = "invalid_server"),
               "'arg' should be one of")

  # Test for invalid prod
  expect_error(create_db_connection(prod = "yes"),
               "'prod' must be a logical value")

  # Test for invalid interactive
  expect_error(create_db_connection(interactive = "no"),
               "'interactive' must be a logical value")
})

test_that("create_db_connection selects correct server configurations", {
  # Mock DBI::dbConnect
  local_mocked_bindings(
    dbConnect = function(...) {
      args <- list(...)
      # Return a list with just the key parameters we care about
      list(
        server = args$server,
        database = args$database,
        Server = args$Server,
        Database = args$Database
      )
    },
    .package = "DBI"
  )

  # Mock odbc::dbConnect
  local_mocked_bindings(
    dbConnect = function(...) {
      args <- list(...)
      # Return a list with just the key parameters we care about
      list(
        Server = args$Server,
        Database = args$Database
      )
    },
    .package = "odbc"
  )

  # Mock keyring functions
  local_mocked_bindings(
    key_list = function(...) list(username = "test_user"),
    key_get = function(...) "test_password",
    .package = "keyring"
  )

  # Test that correct servers are selected based on prod flag
  expect_equal(
    create_db_connection("phextractstore", prod = TRUE)$Server,
    "KCITSQLPRPHIP40"
  )

  expect_equal(
    create_db_connection("phextractstore", prod = FALSE)$Server,
    "KCITSQLUATHIP40"
  )

  # Test that correct database names are used
  hhsaw_conn <- create_db_connection("hhsaw", prod = TRUE)
  expect_equal(hhsaw_conn$database, "hhs_analytics_workspace")

  inthealth_conn <- create_db_connection("inthealth", prod = TRUE)
  expect_equal(inthealth_conn$database, "inthealth_edw")
})

# Run additional manual tests in /tests/manual/test_connections
