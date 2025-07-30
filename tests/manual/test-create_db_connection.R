# Set up for manual testing of create_db_connection() ----
#' This script tests all combinations of connection parameters.
#' Run this manually when you have proper credentials set up.
#' DO NOT run this in automated testing environments (i.e., don't put in /tests/testthat)

library(data.table)
library(DBI)
library(apde.etl)


# Function test_single_connection() ----
#' Test a single database connection
#'
#' @param server The server to connect to
#' @param prod Whether to use production or development
#' @param interactive Whether to use interactive authentication
test_single_connection <- function(server = "phextractstore",
                                   prod = FALSE,
                                   interactive = FALSE) {
  message(paste0("\n--- Testing connection to ", server,
                 " (prod=", prod, ", interactive=", interactive, ") ---"))

  tryCatch({
    conn <- create_db_connection(server, prod, interactive)
    result <- dbGetQuery(conn, "SELECT 1 AS test")

    if (identical(result$test, 1L)) {
      message("\U0001f642 Connection successful!")
    } else {
      message("\U0001f47f Connection issue: Unexpected result from query")
    }

    dbDisconnect(conn)
  }, error = function(e) {
    message(paste0("\U0001f47f Connection failed: ", e$message))
  })

  invisible(NULL)
}

# Function test_all_connections() -- batch tests all connection parameter combinations ----
test_all_connections <- function() {
  # Create a grid of all test combinations
  dt <- CJ(xserver = c("phextractstore", "hhsaw", "inthealth"),
           xprod = c(TRUE, FALSE),
           xinteractive = c(TRUE, FALSE))

  # Loop through each combination and test the connection
  for (i in 1:nrow(dt)) {
    # Extract values for the current iteration
    server_val <- dt[i]$xserver
    prod_val <- dt[i]$xprod
    interactive_val <- dt[i]$xinteractive

    # Use the single connection test function
    test_single_connection(
      server = server_val,
      prod = prod_val,
      interactive = interactive_val
    )

    # Add a sleep to not overwhelm the servers
    Sys.sleep(1)
  }

  message("\n--- All connection tests completed ---")
}

# Use test_all_connections() ----
if (interactive()) {
  # Prompt user before running all tests
  cat("This will test all connection combinations.\n")
  cat("Make sure you have proper credentials set up.\n")
  cat("Do you want to continue? (y/n): ")

  answer <- tolower(readline())
  if (answer == "y" || answer == "yes") {
    test_all_connections()
  } else {
    message("Tests aborted.")
  }
}
