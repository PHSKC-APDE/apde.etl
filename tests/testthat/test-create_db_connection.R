test_that("create_db_connection is deprecated", {
  local_mocked_bindings(connect = function(...) list(...), .package = "apde.connect")

  lifecycle::expect_deprecated(create_db_connection("hhsaw"))
})

test_that("create_db_connection validates arguments correctly", {
  old_opts <- options(lifecycle_verbosity = "quiet")
  on.exit(options(old_opts), add = TRUE)

  # Test for invalid server
  expect_error(create_db_connection(server = "invalid_server"),
               "'arg' should be one of")

  # Test for invalid prod
  expect_error(create_db_connection(prod = "yes"),
               "'prod' must be a logical value")
  expect_error(create_db_connection(prod = NA),
               "'prod' must be a logical value")
})

test_that("create_db_connection passes the correct arguments to apde.connect::connect()", {
  # Mock apde.connect::connect() so it returns the arguments it receives
  local_mocked_bindings(connect = function(...) list(...), .package = "apde.connect")
  old_opts <- options(lifecycle_verbosity = "quiet")
  on.exit(options(old_opts), add = TRUE)

  expect_equal(create_db_connection("hhsaw"),
               list(database = "hhsaw", prod = TRUE, auth = "auto"))

  expect_equal(create_db_connection("hhsaw", prod = FALSE),
               list(database = "hhsaw", prod = FALSE, auth = "auto"))

  expect_equal(create_db_connection("inthealth"),
               list(database = "inthealth_edw", prod = TRUE, auth = "auto"))

  expect_equal(create_db_connection("phextractstore", prod = FALSE),
               list(database = "phextractstore", prod = FALSE, auth = "auto"))

  # default server is phextractstore (first option), as before
  expect_equal(create_db_connection()$database, "phextractstore")
})

test_that("create_db_connection ignores `interactive` without any extra warning", {
  local_mocked_bindings(connect = function(...) list(...), .package = "apde.connect")
  old_opts <- options(lifecycle_verbosity = "quiet") # silence the function-level deprecation warning
  on.exit(options(old_opts), add = TRUE)

  for (server in c("hhsaw", "inthealth", "phextractstore")) {
    for (prod in c(TRUE, FALSE)) {
      expect_no_warning(con_true <- create_db_connection(server, prod = prod, interactive = TRUE))
      expect_identical(con_true, create_db_connection(server, prod = prod, interactive = FALSE))
    }
  }

  # non-logical values are ignored too
  expect_no_error(create_db_connection("hhsaw", interactive = "yes"))
})

# Run additional manual tests in /tests/manual/test-create_db_connection.R
