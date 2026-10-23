# Helpers for tests that need a real database connection ----
# testthat loads helper-*.R files automatically before running tests.
#
# Tests that need a database call skip_if_no_db(). It tries to connect once per
# database/server combination (results are cached for the session) and skips
# the test if that fails, e.g., on a laptop without database access or off the
# KC network.

db_probe_cache <- new.env(parent = emptyenv())

# skip_if_no_db() ----
# Skips the current test if apde.connect::connect() cannot reach `database`
skip_if_no_db <- function(database, prod = TRUE) {
  key <- paste(database, prod, sep = "_")          # e.g. "hhsaw_TRUE" or "hhsaw_FALSE"
  if (!exists(key, envir = db_probe_cache)) {      # first time we've been asked about this one?
    result <- tryCatch({
      con <- apde.connect::connect(database = database, prod = prod,
                                   auth = "auto", timeout = 10)  # try to connect, give up after 10 seconds
      DBI::dbDisconnect(con)                       # it worked, so close it right away
      TRUE
    }, error = function(e) FALSE)                  # any error means it can't connect
    assign(key, result, envir = db_probe_cache)    # remember the answer
  }

  if (get(key, envir = db_probe_cache)) return(invisible(TRUE)) # can connect, so run the test

  testthat::skip(paste0("Cannot connect to ", database, if (prod) " (prod)" else " (dev)")) # cannot connect, so skip the test (it is reported as skipped, not failed)
}
