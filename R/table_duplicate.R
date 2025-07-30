# table_duplicate----
#' Copy a (smaller) SQL table from one server to another
#'
#' @description
#' Copies one or more small SQL tables from a source to a destination database,
#' with options to customize names, preserve or replace existing tables, and
#' copy structure only.
#'
#' @details
#' You can provide table duplication information in two ways:
#'
#' 1. **Batch Mode**: Pass a `table_df` with one or more rows.
#'    Columns `from_schema` and `from_table` are required; `to_schema`
#'    and `to_table` are optional. Any missing values will be filled in using
#'    the corresponding standalone arguments.
#' 2. **Single Table Mode**: Leave `table_df` empty and supply
#'    `from_schema`, `from_table`, and optionally `to_schema` and
#'    `to_table`.
#'
#' **IMPORTANT DEPENDENCY:** This function uses BCP (Bulk Copy Program) for data
#' transfer. See [load_df_bcp()] for installation instructions and Azure
#' setup requirements.
#'
#' The function automatically creates the destination table structure by querying
#' the source table's schema information. If the destination table already exists,
#' it will be compared with the source data and either renamed (default) or
#' deleted based on the `delete_table` parameter.
#'
#' This function is designed for smaller reference or lookup tables. For larger
#' datasets, a more robust process is recommended.
#'
#' @note
#' This function replaces the deprecated `table_duplicate_f()` function from the
#' `apde` package.
#'
#' @param conn_from Name of the connection to the FROM SQL database
#' @param conn_to Name of the connection to the TO SQL database
#' @param server_to Name of the server/odbc for the TO SQL database.
#'
#' **IMPORTANT** For any Azure based duplication, this must be an ODBC DSN name
#' that you create first (see [load_df_bcp()] for setup instructions), and there
#' must be a keyring entry with a service name that matches this parameter.
#' Use [apde_keyring_set()] or `keyring::key_set("your_dsn_name")` to store credentials.
#' @param db_to Name of the TO SQL database
#' @param table_df Optional data.frame listing tables to duplicate. Must contain
#' at least `from_schema` and `from_table` columns. Optional columns:
#' `to_schema`, `to_table`. If values are missing in this data frame,
#' they will be filled using the respective `from_schema`, `from_table`,
#' `to_schema`, and `to_table` arguments.
#' @param from_schema Schema name of the source table. Required if `table_df`
#' is empty or missing the `from_schema` column. Used as a fallback if any
#' row in `table_df$from_schema` is missing or `NA`.
#' @param from_table Name of the source table. Required if `table_df` is empty
#' or missing the `from_table` column. Used as a fallback if any row in
#' `table_df$from_table` is missing or `NA`.
#' @param to_schema Schema name of the destination table. Optional. If not
#' provided, defaults to `from_schema`. Used as a fallback if any row in
#' `table_df$to_schema` is missing or `NA`.
#' @param to_table Name of the destination table. Optional. If not provided,
#' defaults to `from_table`. Used as a fallback if any row in
#' `table_df$to_table` is missing or `NA`.
#' @param to_table_prefix Optional character prefix to be added before every
#' `to_table` name in the destination database.
#' @param confirm_tables If `TRUE`, will require user confirm the list of
#' tables being duplicated
#' @param delete_table If `TRUE`, will delete old TO tables, if `FALSE`,
#' will rename old TO tables
#' @param delete_table_suffix Variable of a suffix to be added after the name of
#' old TO tables
#' @param table_structure_only If `TRUE`, will only copy the table structure
#' without data
#'
#' @return None
#'
#' @author Jeremy Whitehurst, 2024-05-01
#'
#' @seealso
#' - [load_df_bcp()] for BCP installation and Azure setup requirements
#' - [table_duplicate_delete()] for cleaning up tables with specified suffixes
#' - [apde_keyring_set()] for setting up keyring credentials for Azure connections
#'
#' @export
#'
#' @examples
#'  \dontrun{
#'   # Note: Requires BCP utility installation and proper database connections
#'
#'   # Setup connections
#'   conn_from <- create_db_connection("source_server", prod = FALSE)
#'   conn_to <- create_db_connection("dest_server", prod = FALSE)
#'
#'   # Example 1: Simple single table duplication
#'   table_duplicate(conn_from = conn_from,
#'                   conn_to = conn_to,
#'                   server_to = "dest_server_dsn",
#'                   db_to = "target_database",
#'                   from_schema = "source_schema",
#'                   from_table = "lookup_table",
#'                   to_schema = "dest_schema",
#'                   to_table = "lookup_table_copy",
#'                   confirm_tables = FALSE)
#'
#'   # Example 2: Structure only (no data)
#'   table_duplicate(conn_from = conn_from,
#'                   conn_to = conn_to,
#'                   server_to = "dest_server_dsn",
#'                   db_to = "target_database",
#'                   from_schema = "source_schema",
#'                   from_table = "reference_table",
#'                   table_structure_only = TRUE,
#'                   confirm_tables = FALSE)
#'
#'   # Example 3: Batch mode with multiple tables
#'   tables_to_copy <- data.frame(
#'     from_schema = c("schema1", "schema1", "schema2"),
#'     from_table = c("table1", "table2", "table3"),
#'     to_schema = c("dest_schema", "dest_schema", "dest_schema"),
#'     to_table = c("new_table1", "new_table2", "new_table3")
#'   )
#'
#'   table_duplicate(conn_from = conn_from,
#'                   conn_to = conn_to,
#'                   server_to = "dest_server_dsn",
#'                   db_to = "target_database",
#'                   table_df = tables_to_copy,
#'                   confirm_tables = TRUE)
#'
#'   # Example 4: With table prefix for organizational purposes
#'   table_duplicate(conn_from = conn_from,
#'                   conn_to = conn_to,
#'                   server_to = "dest_server_dsn",
#'                   db_to = "target_database",
#'                   from_schema = "source_schema",
#'                   from_table = "data_table",
#'                   to_table_prefix = "backup_",
#'                   confirm_tables = FALSE)
#'
#'   # Don't forget to disconnect
#'   DBI::dbDisconnect(conn_from)
#'   DBI::dbDisconnect(conn_to)
#'  }
#'
table_duplicate <- function(conn_from,
                            conn_to,
                            server_to,
                            db_to,
                            table_df = data.frame(),
                            from_schema = NULL,
                            from_table = NULL,
                            to_schema = NULL,
                            to_table = NULL,
                            to_table_prefix = NULL,
                            confirm_tables = TRUE,
                            delete_table = FALSE,
                            delete_table_suffix = "_dupe_table_to_delete",
                            table_structure_only = FALSE
) {
  if(nrow(table_df) == 0) {
    # Check if table_df is empty. If it is, all from/to variables must be set.
    if(is.null(from_schema) || is.null(from_table)) {
      stop("If the data.frame, table_df, is empty, the variables from_schema and from_table must have values!")
    }
    if(is.null(to_schema)) {
      to_schema = from_schema
    }
    if(is.null(to_table)) {
      to_table = from_table
    }
    # Populate row 1 of table_df with from/to variables.
    table_df[1, "from_schema"] <- from_schema
    table_df[1, "from_table"] <- from_table
    table_df[1, "to_schema"] <- to_schema
    table_df[1, "to_table"] <- to_table
  } else {
    # Check that either table_df has the from/to columns OR the corresponding from/to variable is set.
    if(!"from_schema" %in% colnames(table_df) && is.null(from_schema)) {
      stop("If the data.frame, table_df, is missing the from_schema column, the from_schema variable must have a value!")
    }
    if(!"from_table" %in% colnames(table_df) && is.null(from_table)) {
      stop("If the data.frame, table_df, is missing the from_table column, the from_table variable must have a value!")
    }
  }

  # Populating any missing values in table_df and adding the to_table_prefix
  for(i in 1:nrow(table_df)) {
    if(!"from_schema" %in% colnames(table_df) || is.null(table_df[i, "from_schema"])) {
      table_df[i, "from_schema"] <- from_schema
    }
    if(!"from_table" %in% colnames(table_df) || is.null(table_df[i, "from_table"])) {
      table_df[i, "from_table"] <- from_table
    }
    if(!"to_schema" %in% colnames(table_df) || is.na(table_df[i, "to_schema"])) {
      if(!is.null(to_schema)) {
        table_df[i, "to_schema"] <- to_schema
      } else {
        table_df[i, "to_schema"] <- table_df[i, "from_schema"]
      }
    }
    if(!"to_table" %in% colnames(table_df) || is.na(table_df[i, "to_table"])) {
      if(!is.null(to_table)) {
        table_df[i, "to_table"] <- to_table
      } else {
        table_df[i, "to_table"] <- table_df[i, "from_table"]
      }
    }
    if(!is.null(to_table_prefix)) {
      table_df[i, "to_table"] <- paste0(to_table_prefix, table_df[i, "to_table"])
    }
  }

  # List tables to duplicate.
  message(glue::glue("Ready to duplicate the following {nrow(table_df)} table(s):"))
  for(i in 1:nrow(table_df)) {
    message(glue::glue("{i}: [{table_df[i, 'from_schema']}].[{table_df[i, 'from_table']}] -> [{table_df[i, 'to_schema']}].[{table_df[i, 'to_table']}]"))
  }

  # If confirm_tables == TRUE, user must confirm the tables to duplicate.
  if(confirm_tables == TRUE) {
    confirm <- utils::askYesNo("Duplicate tables?")
    if(confirm == FALSE || is.na(confirm)) {
      stop("Table duplication cancelled.")
    }
  }

  message("Begin duplicating tables...")
  if(table_structure_only == FALSE) {
    for(i in 1:nrow(table_df)) {
      message(glue::glue("Table {i}: [{table_df[i, 'from_schema']}].[{table_df[i, 'from_table']}] -> [{table_df[i, 'to_schema']}].[{table_df[i, 'to_table']}]"))
      message("Pulling data from source table...")
      data_from <- DBI::dbGetQuery(conn_from,
                                   glue::glue_sql("SELECT * FROM {`table_df[i, 'from_schema']`}.{`table_df[i, 'from_table']`}",
                                                  .con = conn_from))
      message("Checking if destination table exists...")
      if(DBI::dbExistsTable(conn_to, name = DBI::Id(schema = table_df[i, "to_schema"], table = table_df[i, "to_table"])) == TRUE) {
        message("Destination table exists. Pulling data from destination table to compare with source table...")
        data_to <- DBI::dbGetQuery(conn_to,
                                   glue::glue_sql("SELECT * FROM {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`}",
                                                  .con = conn_to))
        if(nrow(data_to) == 0) {
          table_match <- FALSE
        } else {
          suppressWarnings(table_match <- dplyr::all_equal(data_from, data_to, ignore_col_order = FALSE))
        }
        if(table_match == TRUE) {
          message("Destination table matches source table...")
          next
        } else {
          message("Destination table does not match source table...")
          if(delete_table == TRUE) {
            message("Deleting old destination table...")
            DBI::dbExecute(conn_to,
                           glue::glue_sql("DROP TABLE {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`}",
                                          .con = conn_to))
          } else {
            dts <- delete_table_suffix
            dts_num <- 0
            # Checks if there is already a "to_delete" table with the same name. If so, keep adding a number to the end until you are renaming to a new table.
            while(DBI::dbExistsTable(conn_to, name = DBI::Id(schema = table_df[i, "to_schema"], table = paste0(table_df[i, "to_table"], dts))) == TRUE) {
              dts_num <- dts_num + 1
              dts <- paste0(delete_table_suffix, "_", dts_num)
            }
            message(glue::glue("Renaming old destination table to [{table_df[i, 'to_schema']}].[{paste0(table_df[i, 'to_table'], dts)}]..."))
            # Attempts to rename table with syntax for standard databases. If that fails, rename table with syntax that is used in an Azure Synapse environment.
            tryCatch(
              {
                DBI::dbExecute(conn_to,
                               glue::glue_sql("EXEC sp_rename {paste0(table_df[i, 'to_schema'], '.', table_df[i, 'to_table'])}, {paste0(table_df[i, 'to_table'], dts)}",
                                              .con = conn_to))
              },
              error = function(cond) {
                DBI::dbExecute(conn_to,
                               glue::glue_sql("RENAME OBJECT {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`} TO {`{paste0(table_df[i, 'to_table'], dts)}`}",
                                              .con = conn_to))
              }
            )
          }
        }
      } else {
        message("Destination table does not exist...")
      }
      cols_from <- DBI::dbGetQuery(conn_from,
                                   glue::glue_sql("
                                                  SELECT
                                                  [COLUMN_NAME],
                                                  [DATA_TYPE],
                                                  [CHARACTER_MAXIMUM_LENGTH],
                                                  [NUMERIC_PRECISION],
                                                  [NUMERIC_SCALE],
                                                  [CHARACTER_SET_NAME],
                                                  [COLLATION_NAME],
                                                  CONCAT(
                                                    '[', [COLUMN_NAME], '] ',
                                                    UPPER([DATA_TYPE]),
	                                                  CASE
                                                  		WHEN [DATA_TYPE] IN('VARCHAR', 'CHAR', 'NVARCHAR') THEN CONCAT('(',CASE
                                                  		                                                                    WHEN [CHARACTER_MAXIMUM_LENGTH] = -1 THEN 'MAX'
                                                  		                                                                    ELSE CAST([CHARACTER_MAXIMUM_LENGTH] AS VARCHAR(4))
                                                  		                                                                  END
                                                  		                                                                  , ') COLLATE ', [COLLATION_NAME])
                                                  		WHEN [DATA_TYPE] IN('DECIMAL', 'NUMERIC') THEN CONCAT('(', [NUMERIC_PRECISION], ',', [NUMERIC_SCALE], ')')
                                                  		ELSE ''
                                                  	END,
	                                                  ' NULL') AS 'COLUMN_DEFINITION'
                                                FROM [INFORMATION_SCHEMA].[COLUMNS]
                                                WHERE
                                                  [TABLE_NAME] = {table_df[i, 'from_table']}
                                                  AND [TABLE_SCHEMA] = {table_df[i, 'from_schema']}
                                                ORDER BY [ORDINAL_POSITION]",
                                                  .con = conn_from))
      message("Creating destination table...")
      create_code <- glue::glue_sql(
        "CREATE TABLE {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`}
        ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{DBI::SQL(cols_from$COLUMN_DEFINITION)}',.con = conn_to), sep = ', \n  '))})",
        .con = conn_to)

      DBI::dbExecute(conn_to, create_code)
      message("Copying source data to destination table...")
      data_from <- dplyr::mutate_all(data_from, as.character)
      if(length(keyring::key_list(server_to)[["username"]]) == 0) {
        u <- NULL
        p <- NULL
      } else {
        u <- keyring::key_list(server_to)[["username"]]
        p <- keyring::key_get(server_to, keyring::key_list(server_to)[["username"]])
      }
      load_df_bcp(dataset = data_from,
                  server = server_to,
                  db_name = db_to,
                  schema_name = table_df[i, "to_schema"],
                  table_name = table_df[i, "to_table"],
                  user = u,
                  pass = p)
      message("Table duplication complete...")
    }
  } else {
    for(i in 1:nrow(table_df)) {
      message(glue::glue("Table {i}: [{table_df[i, 'from_schema']}].[{table_df[i, 'from_table']}] -> [{table_df[i, 'to_schema']}].[{table_df[i, 'to_table']}]"))
      message("Checking if destination table exists...")
      if(DBI::dbExistsTable(conn_to, name = DBI::Id(schema = table_df[i, "to_schema"], table = table_df[i, "to_table"])) == TRUE) {
        if(delete_table == TRUE) {
          message("Deleting old destination table...")
          DBI::dbExecute(conn_to,
                         glue::glue_sql("DROP TABLE {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`}",
                                        .con = conn_to))
        } else {
          dts <- delete_table_suffix
          dts_num <- 0
          # Checks if there is already a "to_delete" table with the same name. If so, keep adding a number to the end until you are renaming to a new table.
          while(DBI::dbExistsTable(conn_to, name = DBI::Id(schema = table_df[i, "to_schema"], table = paste0(table_df[i, "to_table"], dts))) == TRUE) {
            dts_num <- dts_num + 1
            dts <- paste0(delete_table_suffix, "_", dts_num)
          }
          message(glue::glue("Renaming old destination table to [{table_df[i, 'to_schema']}].[{paste0(table_df[i, 'to_table'], dts)}]..."))
          # Attempts to rename table with syntax for standard databases. If that fails, rename table with syntax that is used in an Azure Synapse environment.
          tryCatch(
            {
              DBI::dbExecute(conn_to,
                             glue::glue_sql("EXEC sp_rename {paste0(table_df[i, 'to_schema'], '.', table_df[i, 'to_table'])}, {paste0(table_df[i, 'to_table'], dts)}",
                                            .con = conn_to))
            },
            error = function(cond) {
              DBI::dbExecute(conn_to,
                             glue::glue_sql("RENAME OBJECT {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`} TO {`{paste0(table_df[i, 'to_table'], dts)}`}",
                                            .con = conn_to))
            }
          )
        }
      } else {
        message("Destination table does not exist...")
      }
      cols_from <- DBI::dbGetQuery(conn_from,
                                   glue::glue_sql("
                                                  SELECT
                                                  [COLUMN_NAME],
                                                  [DATA_TYPE],
                                                  [CHARACTER_MAXIMUM_LENGTH],
                                                  [NUMERIC_PRECISION],
                                                  [NUMERIC_SCALE],
                                                  [CHARACTER_SET_NAME],
                                                  [COLLATION_NAME],
                                                  CONCAT(
                                                    '[', [COLUMN_NAME], '] ',
                                                    UPPER([DATA_TYPE]),
	                                                  CASE
                                                  		WHEN [DATA_TYPE] IN('VARCHAR', 'CHAR', 'NVARCHAR') THEN CONCAT('(',CASE
                                                  		                                                                    WHEN [CHARACTER_MAXIMUM_LENGTH] = -1 THEN 'MAX'
                                                  		                                                                    ELSE CAST([CHARACTER_MAXIMUM_LENGTH] AS VARCHAR(4))
                                                  		                                                                  END
                                                  		                                                                  , ') COLLATE ', [COLLATION_NAME])
                                                  		WHEN [DATA_TYPE] IN('DECIMAL', 'NUMERIC') THEN CONCAT('(', [NUMERIC_PRECISION], ',', [NUMERIC_SCALE], ')')
                                                  		ELSE ''
                                                  	END,
	                                                  ' NULL') AS 'COLUMN_DEFINITION'
                                                FROM [INFORMATION_SCHEMA].[COLUMNS]
                                                WHERE
                                                  [TABLE_NAME] = {table_df[i, 'from_table']}
                                                  AND [TABLE_SCHEMA] = {table_df[i, 'from_schema']}
                                                ORDER BY [ORDINAL_POSITION]",
                                                  .con = conn_from))
      message("Creating destination table...")
      create_code <- glue::glue_sql(
        "CREATE TABLE {`table_df[i, 'to_schema']`}.{`table_df[i, 'to_table']`}
        ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{DBI::SQL(cols_from$COLUMN_DEFINITION)}',.con = conn_to), sep = ', \n  '))})",
        .con = conn_to)

      DBI::dbExecute(conn_to, create_code)
    }
  }
  message("All tables duplicated successfully.")
}



# table_duplicate_delete() ----
#' Delete SQL tables matching a specified suffix pattern
#'
#' @description
#' Identifies and deletes SQL tables whose names contain a specified suffix
#' pattern. Includes user confirmation before deletion to prevent accidental
#' data loss.
#'
#' @param conn Name of the connection to the SQL database
#' @param delete_table_suffix String pattern to match table names for deletion.
#' Uses SQL LIKE pattern matching with wildcards. Default is
#' `"_dupe_table_to_delete"`.
#'
#' @details
#' This function queries the INFORMATION_SCHEMA.TABLES to find tables whose names
#' contain the given suffix pattern using SQL LIKE matching (e.g., `%suffix%`).
#' It presents a list of matching tables and requires user confirmation before
#' performing the deletion.
#'
#' **Warning:** This function permanently deletes tables. Always review the list
#' of tables to be deleted before confirming.
#'
#' @note
#' This function replaces the deprecated `table_duplicate_delete_f()` function from the
#' `apde` package.
#'
#' @return None (invisible). Tables are deleted from the database.
#'
#' @author Jeremy Whitehurst, 2024-05-01
#'
#' @seealso [table_duplicate()] for creating tables that may need cleanup
#'
#' @export
#'
#' @examples
#'  \dontrun{
#'   # Setup connection
#'   conn <- create_db_connection("my_server", prod = FALSE)
#'
#'   # Delete tables with default suffix
#'   table_duplicate_delete(conn)
#'
#'   # Delete tables with custom suffix
#'   table_duplicate_delete(conn, "_backup_old")
#'
#'   # Disconnect when done
#'   DBI::dbDisconnect(conn)
#'  }

table_duplicate_delete <- function(conn,
                                   delete_table_suffix = "_dupe_table_to_delete"
) {
  tables <- DBI::dbGetQuery(conn,
                            glue::glue_sql("SELECT * FROM [INFORMATION_SCHEMA].[TABLES]
                                           WHERE [TABLE_NAME] LIKE {paste0('%', delete_table_suffix, '%')}
                                           ORDER BY TABLE_SCHEMA, TABLE_NAME",
                                           .con = conn))
  message(glue::glue("There are {nrow(tables)} table(s) to delete:"))
  for(i in 1:nrow(tables)) {
    message(glue::glue("{i}: [{tables[i, 'TABLE_SCHEMA']}].[{tables[i, 'TABLE_NAME']}]"))
  }
  confirm <- utils::askYesNo("Delete tables?")
  if(confirm == FALSE || is.na(confirm)) {
    stop("Table deletion cancelled.")
  } else {
    for(i in 1:nrow(tables)) {
      DBI::dbExecute(conn,
                     glue::glue_sql("DROP TABLE {`tables[i, 'TABLE_SCHEMA']`}.{`tables[i, 'TABLE_NAME']`}",
                                    .con = conn))
    }
    message("Table(s) deleted.")
  }
}
