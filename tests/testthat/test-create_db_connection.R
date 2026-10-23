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

# Test if connect to the actual database ----
# Should skip if that database cannot be reached (see helper-db.R).

real_db_cases <- data.frame(
  server = c("hhsaw", "hhsaw", "inthealth", "inthealth", "phextractstore", "phextractstore"),
  prod = c(TRUE, FALSE, TRUE, FALSE, TRUE, FALSE),
  database = c("hhsaw", "hhsaw", "inthealth_edw", "inthealth_edw", "phextractstore", "phextractstore"),
  dbname = c("hhs_analytics_workspace", "hhs_analytics_workspace", "inthealth_edw", "inthealth_edw", "PHExtractStore", "PHExtractStore"),
  host = c("kcitazrhpasqlprp16", "kcitazrhpasqldev20", "kcitazrhpasqlprp16", "kcitazrhpasqldev20", "KCITSQLPRPHIP40", "KCITSQLUATHIP40")
)

for (i in seq_len(nrow(real_db_cases))) {
  case <- real_db_cases[i, ]

  test_that(paste0("create_db_connection connects to ", case$server,
                   " (", if (case$prod) "prod" else "dev", ")"), {
    skip_if_no_db(case$database, prod = case$prod) # will skip if connection fails
    old_opts <- options(lifecycle_verbosity = "quiet")
    on.exit(options(old_opts), add = TRUE)

    con <- create_db_connection(server = case$server, prod = case$prod)
    on.exit(DBI::dbDisconnect(con), add = TRUE)

    # it is an open connection ...
    expect_true(DBI::dbIsValid(con))

    # ... to the right database and server ...
    info <- DBI::dbGetInfo(con)
    expect_equal(tolower(info$dbname), tolower(case$dbname))
    expect_match(info$servername, case$host, ignore.case = TRUE)

    # ... that can run a query
    expect_equal(DBI::dbGetQuery(con, "SELECT 1 AS test")$test, 1L)
  })
}

test_that("create_db_connection connects with interactive = TRUE (ignored)", {
  skip_if_no_db("hhsaw", prod = TRUE)
  old_opts <- options(lifecycle_verbosity = "quiet")
  on.exit(options(old_opts), add = TRUE)

  con <- create_db_connection(server = "hhsaw", interactive = TRUE)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  expect_true(DBI::dbIsValid(con))
  expect_equal(DBI::dbGetInfo(con)$dbname, "hhs_analytics_workspace")
})

test_that("create_db_connection shows the deprecation warning on a real connection", {
  skip_if_no_db("hhsaw", prod = TRUE)

  lifecycle::expect_deprecated(con <- create_db_connection(server = "hhsaw"))
  DBI::dbDisconnect(con)
})

# Additional manual tests are in /tests/manual/test-create_db_connection.R
