#' Clean up reference address tables
#'
#' @description `deduplicate_addresses` removes duplicate addresses in the ref tables and synchronize.
#'
#' @details This function brings in all addresses currently in the reference tables
#' and deduplicates them. Because there is a stage -> final workflow, the stage version
#' of each table also needs to be deduplicated. Also, the reference tables are currently
#' synchronized between two different servers. In order to avoid reintroducing
#' duplicates, both servers must be deduplicated (and synchronized at the same time).
#' Server and table names are currently hard coded but could be made more generalized if needed.
#'
#' @note
#' This function replaces the deprecated `deduplicate_addresses` function from the
#' `apde` package.
#'
#' @param conn_hhsaw SQL server connection to the HHSAW server, created using `odbc` package.
#' @param conn_phclaims SQL server connection to the PHClaims server, created using `odbc` package.
#'
#' @return No explicit return value. Function performs database operations to deduplicate and synchronize address tables.
#'
#' @examples
#' \dontrun{
#' # Create database connections
#' conn_hhsaw <- DBI::dbConnect(odbc::odbc(), "HHSAW")
#' conn_phclaims <- DBI::dbConnect(odbc::odbc(), "PHClaims")
#'
#' # Deduplicate and synchronize address tables
#' deduplicate_addresses(conn_hhsaw = conn_hhsaw, conn_phclaims = conn_phclaims)
#'
#' # Close connections
#' DBI::dbDisconnect(conn_hhsaw)
#' DBI::dbDisconnect(conn_phclaims)
#' }
#'
#' @export

deduplicate_addresses <- function(conn_hhsaw = NULL,
                                  conn_phclaims = NULL) {

  # Set up functions to avoid duplicating code ----
  sql_loader <- function(df, conn = NULL, to_schema = "ref", to_table = NULL, overwrite = T) {
    start <- 1L
    max_rows <- 50000L
    cycles <- ceiling(nrow(df)/max_rows)

    initial_append <- ifelse(overwrite == TRUE, FALSE, TRUE)

    lapply(seq(start, cycles), function(i) {
      start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
      end_row <- min(nrow(df), max_rows * i)

      message("Loading cycle ", i, " of ", cycles)
      if (i == 1) {
        DBI::dbWriteTable(conn,
                          name = DBI::Id(schema = to_schema,  table = to_table),
                          value = as.data.frame(df[start_row:end_row, ]),
                          overwrite = overwrite, append = initial_append)
      } else {
        DBI::dbWriteTable(conn,
                          name = DBI::Id(schema = to_schema,  table = to_table),
                          value = as.data.frame(df[start_row:end_row ,]),
                          overwrite = F, append = T)
      }
    })
  }


  dedup <- function(conn = NULL, to_schema = "ref", to_table = NULL,
                    grouping = c("geo_hash_raw", "geo_hash_geocode")) {

    grouping <- match.arg(grouping)

    # Bring in data
    message("Bringing in data from ", to_schema, ".", to_table)
    adds <- data.table::setDT(DBI::dbGetQuery(conn,
                                              glue::glue_sql("select * from {`to_schema`}.{`to_table`}",
                                                             .con = conn)))

    # Deduplicate
    adds[, row_cnt := .N, by = grouping]
    print(dplyr::count(adds, row_cnt))

    # See how many duplicates need to be removed
    adds_dups <- nrow(adds[row_cnt > 1])

    if (adds_dups > 1) {
      adds_rows <- nrow(adds)
      message(adds_dups, " rows in ", to_schema, ".", to_table, " will be deduplicated")

      # Sort so that the newer row is prioritized (assume improvements to address cleaning/geocoding)
      order_cols <- c(grouping, "last_run")
      data.table::setorderv(adds, order_cols)
      # Take the first row of each geo_hash
      adds <- adds[adds[, .I[1], by = grouping]$V1]

      message(adds_rows - nrow(adds), " duplicate rows were removed from ", to_schema, ".", to_table)
    } else {
      message("No duplicate rows found in ", to_schema, ".", to_table)
    }

    # Load data back to SQL
    if (adds_dups > 1) {
      adds[, row_cnt := NULL] # Remove column used to find duplicates
      sql_loader(df = adds, conn = conn, to_schema = to_schema, to_table = to_table, overwrite = T)
    }

    # Return the data frame to use it in synchronizing across servers
    return(adds)
  }


  sync <- function(conn_hhsaw, conn_phclaims,
                   df_hhsaw, df_phclaims,
                   to_schema_hhsaw, to_table_hhsaw,
                   to_schema_phclaims, to_table_phclaims,
                   grouping = c("geo_hash_raw", "geo_hash_geocode")) {

    grouping <- match.arg(grouping)

    # Compare and find differences
    update_hhsaw <- dplyr::anti_join(df_phclaims, df_hhsaw, by = grouping)
    update_phclaims <- dplyr::anti_join(df_hhsaw, df_phclaims, by = grouping)

    # Update tables so they are in sync
    if (nrow(update_hhsaw) > 0) {
      message(nrow(update_hhsaw), " address rows to be loaded from PHClaims to HHSAW")
      print(utils::str(update_hhsaw))
      sql_loader(df = update_hhsaw, conn = conn_hhsaw,
                 to_schema = to_schema_hhsaw, to_table = to_table_hhsaw,
                 overwrite = F)
    } else {
      message("No rows to add to HHSAW table")
    }

    if (nrow(update_phclaims) > 0) {
      message(nrow(update_phclaims), " address rows to be loaded from HHSAW to PHClaims")
      print(utils::str(update_phclaims))
      sql_loader(df = update_phclaims, conn = conn_phclaims,
                 to_schema = to_schema_phclaims, to_table = to_table_phclaims,
                 overwrite = F)
    } else {
      message("No rows to add to PHClaims table")
    }
  }


  # ref.address_clean ----
  ## Deduplicate ----
  ### HHSAW data ----
  message("Deduplicating HHSAW tables")
  adds_stage_hhsaw <- dedup(conn = conn_hhsaw, to_schema = "ref", to_table = "stage_address_clean", grouping = "geo_hash_raw")
  adds_hhsaw <- dedup(conn = conn_hhsaw, to_schema = "ref", to_table = "address_clean", grouping = "geo_hash_raw")

  ### PHClaims data ----
  message("Deduplicating PHClaims tables")
  adds_stage_phclaims <- dedup(conn = conn_phclaims, to_schema = "stage", to_table = "address_clean", grouping = "geo_hash_raw")
  adds_phclaims <- dedup(conn = conn_phclaims, to_schema = "ref", to_table = "address_clean", grouping = "geo_hash_raw")


  ## Compare and find differences ----
  message("Synchronizing address tables")
  ### Stage table ----
  sync(conn_hhsaw = conn_hhsaw, conn_phclaims = conn_phclaims,
       df_hhsaw = adds_stage_hhsaw, df_phclaims = adds_stage_phclaims,
       to_schema_hhsaw = "ref", to_table_hhsaw = "stage_address_clean",
       to_schema_phclaims = "stage", to_table_phclaims = "address_clean",
       grouping = "geo_hash_raw")

  ### Final table ----
  sync(conn_hhsaw = conn_hhsaw, conn_phclaims = conn_phclaims,
       df_hhsaw = adds_hhsaw, df_phclaims = adds_phclaims,
       to_schema_hhsaw = "ref", to_table_hhsaw = "address_clean",
       to_schema_phclaims = "ref", to_table_phclaims = "address_clean",
       grouping = "geo_hash_raw")


  # ref.address_geocode ----
  ## Deduplicate ----
  ### HHSAW data ----
  adds_stage_hhsaw <- dedup(conn = conn_hhsaw,
                            to_schema = "ref", to_table = "stage_address_geocode",
                            grouping = "geo_hash_geocode")
  adds_hhsaw <- dedup(conn = conn_hhsaw,
                      to_schema = "ref", to_table = "address_geocode",
                      grouping = "geo_hash_geocode")

  ### PHClaims data ----
  adds_stage_phclaims <- dedup(conn = conn_phclaims,
                               to_schema = "stage", to_table = "address_geocode",
                               grouping = "geo_hash_geocode")
  adds_phclaims <- dedup(conn = conn_phclaims,
                         to_schema = "ref", to_table = "address_geocode",
                         grouping = "geo_hash_geocode")


  ## Compare and find differences ----
  message("Synchronizing geocode tables")
  ### Stage table ----
  sync(conn_hhsaw = conn_hhsaw, conn_phclaims = conn_phclaims,
       df_hhsaw = adds_stage_hhsaw, df_phclaims = adds_stage_phclaims,
       to_schema_hhsaw = "ref", to_table_hhsaw = "stage_address_geocode",
       to_schema_phclaims = "stage", to_table_phclaims = "address_geocode",
       grouping = "geo_hash_geocode")

  ### Final table ----
  sync(conn_hhsaw = conn_hhsaw, conn_phclaims = conn_phclaims,
       df_hhsaw = adds_hhsaw, df_phclaims = adds_phclaims,
       to_schema_hhsaw = "ref", to_table_hhsaw = "address_geocode",
       to_schema_phclaims = "ref", to_table_phclaims = "address_geocode",
       grouping = "geo_hash_geocode")
}
