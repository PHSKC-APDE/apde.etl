#' @title Load data from one SQL table to another
#'
#' @description `load_table_from_sql` loads data from one SQL table to another
#' using specified variables or a YAML config file.
#'
#' @details This function loads data file to an already existing SQL table using
#' specified variables or a YAML configuration file. The function is essentially a
#' shortcut for SQL code to truncate a table and insert new rows, with added functionality
#' for truncating at a certain date and loading from an archive table.
#' Users can specify some input functions (e.g., to_table) and rely on the config file
#' for the rest of the necessary information.
#' For all arguments that could be specified or come from a YAML file, the hierarchy is
#' specified > argument under server in YAML > argument not under server in YAML.
#'
#' @note
#' This function replaces the deprecated `load_table_from_sql()` function from the
#' `apde` package.
#'
#' ## Example YAML file with no server or individual years
#' (Assume the indentation is appropriate)
#' ```
#' from_schema: stage
#' from_table: mcaid_elig
#' to_schema: final
#' to_table: mcaid_elig
#' *optional other components like a qa_schema and qa_table, index name, vars, etc.*
#' ```
#'
#' ## Example YAML file with servers (phclaims, hhsaw) and individual years
#' (Assume the indentation is appropriate)
#' ```
#' phclaims:
#'     from_schema: stage
#'     from_table: mcaid_elig
#'     to_schema: final
#'     to_table: mcaid_elig
#' hhsaw:
#'     from_schema: claims
#'     from_table: stage_mcaid_elig
#'     to_schema: claims
#'     to_table: final_mcaid_elig
#' *optional other components like a qa_schema and qa_table, index name, vars, etc.*
#' ````
#'
#' @param conn SQL server connection created using `odbc` or `DBI` packages.
#' @param server Name of server being used (only applies if using a YAML file).
#' Useful if the same table is loaded to multiple servers but with different names
#' or schema.
#' @param config Name of object in global environment that contains configuration
#' information. Use one of `config`, `config_url`, or `config_file`.
#' Should be in a YAML format with at least the following variables:
#' *from_schema*, *from_table*, *to_schema* and *to_table*.
#' All mandatory variables should all be nested under the server name if applicable,
#' other variables should not.
#' @param config_url URL of a YAML config file. Use one of `config`, `config_url`, or
#' `config_file`. Note the requirements under `config`.
#' @param config_file File path of a YAML config file. Use one of `config`, `config_url`, or
#' `config_file`. Note the requirements under `config`.
#' @param from_schema Name of the schema that data will be loaded from (if not using YAML input).
#' @param from_table Name of the table that data will be loaded from (if not using YAML input).
#' @param to_schema Name of the schema that data will be loaded to (if not using YAML input).
#' @param to_table Name of the table that data will be loaded to (if not using YAML input).
#' @param archive_schema Name of the schema where archived data live (if not using YAML input).
#' Must be provided if using truncate_date (either directly or from the YAML).
#' @param archive_table Name of the table where archived data live (if not using YAML input).
#' Must be provided if using truncate_date (either directly or from the YAML).
#' @param truncate Truncate existing table prior to loading. Default is `FALSE`.
#' @param truncate_date Truncate existing table at a certain date. Assumes existing table has older data.
#' Must provide archive_schema and archive_table values (either directly or from the YAML) if using this
#' option because existing data needs to go somewhere. Default is `FALSE`.
#' @param auto_date Attempt to use from_table data to ascertain the date to use for truncation cutoff. Default is `FALSE`.
#' @param date_var Name of the date variable
#' @param date_cutpoint Date at which to truncate existing data (if not using YAML input or auto_date).
#' @param drop_index Drop any existing indices prior to loading data. This can speed
#' loading times substantially. Use `add_index` to restore the index after. Default is `TRUE`.
#' @param test_schema Write to a temporary/development schema when testing out table creation.
#' Will use the to_schema (specified or in the YAML file) to make a new table name of
#' \{to_schema\}_\{to_table\}. Schema must already exist in the database. Most useful
#' when the user has an existing YAML file and does not want to overwrite it.
#' Only 5,000 rows will be loaded to each table (4000 from the archive table if it exists and 1000 from the
#' from_table). Default is NULL.
#'
#' @examples
#' \dontrun{
#' load_table(conn = db_claims, server = "hhsaw", config = load_config)
#'
#' load_table(conn = db_claims, server = "phclaims",
#'   config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/load_mcaid_raw.yaml",
#'   overall = F, ind_yr = T)
#' }
#'
#' @export

load_table_from_sql <- function(
    conn,
    server = NULL,
    config = NULL,
    config_url = NULL,
    config_file = NULL,
    from_schema = NULL,
    from_table = NULL,
    to_schema = NULL,
    to_table = NULL,
    archive_schema = NULL,
    archive_table = NULL,
    truncate = FALSE,
    truncate_date = FALSE,
    auto_date = FALSE,
    date_var = "from_date",
    date_cutpoint = NULL,
    drop_index = TRUE,
    test_schema = NULL
) {

  # INITIAL ERROR CHECKS ----
  # Check if the config provided is a local file or on a webpage
  if (sum(!is.null(config), !is.null(config_url), !is.null(config_file)) > 1) {
    stop("Specify either a local config object, config_url, or config_file but only one")
  }

  if (!is.null(config_url)) {
    message("Warning: YAML configs pulled from a URL are subject to fewer error checks")
  }

  # Check that the yaml config file exists in the right format
  if (!is.null(config_file)) {
    # Check that the yaml config file exists in the right format
    if (file.exists(config_file) == FALSE) {
      stop("Config file does not exist, check file name")
    }

    if (configr::is.yaml.file(config_file) == FALSE) {
      stop(paste0("Config file is not a YAML config file. \n",
                  "Check there are no duplicate variables listed"))
    }
  }

  if (truncate == TRUE & truncate_date == TRUE) {
    message("Warning: truncate and truncate_date both set to TRUE. \n
          Entire table will be truncated.")
  }


  # SET UP SERVER ----
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }


  # READ IN CONFIG FILE ----
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(httr::GET(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }

  # Make sure a valid URL was found
  if ('404' %in% names(table_config)) {
    stop("Invalid URL for YAML file")
  }


  # TABLE VARIABLES ----
  ## from_schema ----
  if (is.null(from_schema)) {
    if (!is.null(table_config[[server]][["from_schema"]])) {
      from_schema <- table_config[[server]][["from_schema"]]
    } else if (!is.null(table_config$from_schema)) {
      from_schema <- table_config$from_schema
    }
  }

  ## from_table ----
  if (is.null(from_table)) {
    if (!is.null(table_config[[server]][["from_table"]])) {
      from_table <- table_config[[server]][["from_table"]]
    } else if (!is.null(table_config$from_table)) {
      from_table <- table_config$from_table
    }
  }

  ## to_schema ----
  if (is.null(to_schema)) {
    if (!is.null(table_config[[server]][["to_schema"]])) {
      to_schema <- table_config[[server]][["to_schema"]]
    } else if (!is.null(table_config$to_schema)) {
      to_schema <- table_config$to_schema
    }
  }

  ## to_table ----
  if (is.null(to_table)) {
    if (!is.null(table_config[[server]][["to_table"]])) {
      to_table <- table_config[[server]][["to_table"]]
    } else if (!is.null(table_config$to_table)) {
      to_table <- table_config$to_table
    }
  }

  ## archive_schema ----
  if (is.null(archive_schema)) {
    if (!is.null(table_config[[server]][["archive_schema"]])) {
      archive_schema <- table_config[[server]][["archive_schema"]]
    } else if (!is.null(table_config$archive_schema)) {
      archive_schema <- table_config$archive_schema
    }
  }

  ## archive_table ----
  if (is.null(archive_table)) {
    if (!is.null(table_config[[server]][["archive_table"]])) {
      archive_table <- table_config[[server]][["archive_table"]]
    } else if (!is.null(table_config$archive_table)) {
      archive_table <- table_config$archive_table
    }
  }


  # ADDITIONAL ERROR CHECKS ----
  if (truncate_date == TRUE & (is.null(archive_schema) | is.null(archive_table))) {
    stop("archive_schema and archive_table required when truncate_date = T")
  }


  # TEST MODE ----
  if (!is.null(test_schema)) {
    message("FUNCTION WILL BE RUN IN TEST MODE, WRITING TO ", toupper(test_schema), " SCHEMA")
    test_msg <- " (function is in test mode, only 5,000 rows will be loaded)"
    # Overwrite existing values (order matters here)
    to_table <- glue::glue("{to_schema}_{to_table}")
    to_schema <- test_schema
    archive_schema <- test_schema
    archive_table <- glue::glue("archive_{to_table}")
    load_rows <- " TOP (5000) " # Using 5,000 to better test data from multiple years
    if (!is.null(archive_schema)) {
      archive_rows <- " TOP (4000) " # When unioning tables in test mode, ensure a mix from both
      new_rows <- " TOP (1000) " # When unioning tables in test mode, ensure a mix from both
    } else {
      archive_rows <- ""
      new_rows <- " TOP (5000) " # When unioning tables in test mode, ensure a mix from both
    }
  } else {
    test_msg <- ""
    load_rows <- ""
    archive_rows <- ""
    new_rows <- ""
  }


  # DATE TRUNCATION ----
  if (truncate_date == TRUE) {

    if (is.null(date_var)) {
      if (!is.null(table_config[[server]][["date_var"]])) {
        date_var <- table_config[[server]][["date_var"]]
      } else if (!is.null(table_config$date_var)) {
        date_var <- table_config$date_var
      } else {
        stop("No date_var variable specified. This is needed when truncate_date = TRUE")
      }
    }

    if (auto_date == TRUE) {
      if (!is.null(date_cutpoint)) {
        warning("auto_date = T and date_cutpoint provided, using auto_date")
      }
      # Find the most recent date in the new data
      date_cutpoint <- DBI::dbGetQuery(conn, glue::glue_sql("SELECT MAX({`date_var`})
                                 FROM {`from_schema`}.{`from_table`}",
                                                  .con = conn))
    } else {
      if (is.null(date_cutpoint)) {
        if (!is.null(table_config[[server]][["date_cutpoint"]])) {
          date_cutpoint <- table_config[[server]][["date_cutpoint"]]
        } else if (!is.null(table_config$date_cutpoint)) {
          date_cutpoint <- table_config$date_cutpoint
        } else {
          stop("No date_cutpoint variable specified. This is needed when truncate_date = TRUE and auto_date = FALSE")
        }
      }
    }

    message(glue::glue("Date to truncate from: {date_cutpoint}"))
  }


  # DEAL WITH EXISTING TABLE ----
  # Make sure temp table exists if needed
  if (!is.null(test_schema)) {
    if (DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table)) == FALSE) {
      stop("The temporary to_table (", to_schema, ".", to_table, ") does not exist. Create table and run again.")
    }
  }

  ## Truncate existing table if desired ----
  if (truncate == TRUE) {
    DBI::dbGetQuery(conn, glue::glue_sql("TRUNCATE TABLE {`to_schema`}.{`to_table`}", .con = conn))
  }

  # 'Truncate' from a given date if desired (really move existing data to archive then copy back)
  if (truncate == FALSE & truncate_date == TRUE) {
    # Check if the archive table exists and move table over. If not, show message.
    if (DBI::dbExistsTable(conn, DBI::Id(schema = archive_schema, table = archive_table))) {
      message("Truncating existing archive table")
      DBI::dbGetQuery(conn, glue::glue_sql("TRUNCATE TABLE {`archive_schema`}.{`archive_table`}", .con = conn))
    } else {
      stop(archive_schema, ".", archive_table, " does not exist, please create it")
    }

    # Use real to_schema and to_table here to obtain actual data
    sql_archive <- glue::glue_sql("INSERT INTO {`archive_schema`}.{`archive_table`} WITH (TABLOCK)
                                SELECT {`archive_rows`} * FROM {`to_schema`}.{`to_table`}",
                                  .con = conn,
                                  archive_rows = DBI::SQL(archive_rows))

    message("Archiving existing table")
    DBI::dbGetQuery(conn, sql_archive)


    # Check that the full number of rows are in the archive table
    if (is.null(test_schema)) {
      archive_row_cnt <- as.numeric(DBI::dbGetQuery(
        conn, glue::glue_sql("SELECT COUNT (*) FROM {`archive_schema`}.{`archive_table`}", .con = conn)))
      stage_row_cnt <- as.numeric(DBI::dbGetQuery(
        conn, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = conn)))

      if (archive_row_cnt != stage_row_cnt) {
        stop("The number of rows differ between ", archive_schema, " and ", to_schema, " schemas")
      }
    }

    # Now truncate destination table
    DBI::dbGetQuery(conn, glue::glue_sql("TRUNCATE TABLE {`to_schema`}.{`to_table`}", .con = conn))
  }


  ## Remove existing clustered index if desired ----
  if (drop_index == TRUE) {
    # This code pulls out the clustered index name
    index_sql <- glue::glue_sql("SELECT DISTINCT a.index_name
                                  FROM
                                  (SELECT ind.name AS index_name
                                  FROM
                                  (SELECT object_id, name, type_desc FROM sys.indexes
                                  WHERE type_desc LIKE 'CLUSTERED%') ind
                                  INNER JOIN
                                  (SELECT name, schema_id, object_id FROM sys.tables
                                  WHERE name = {`table`}) t
                                  ON ind.object_id = t.object_id
                                  INNER JOIN
                                  (SELECT name, schema_id FROM sys.schemas
                                  WHERE name = {`schema`}) s
                                  ON t.schema_id = s.schema_id
                                  ) a", .con = conn,
                                table = DBI::dbQuoteString(conn, to_table),
                                schema = DBI::dbQuoteString(conn, to_schema))

    index_name <- DBI::dbGetQuery(conn, index_sql)[[1]]

    if (length(index_name) != 0) {
      DBI::dbGetQuery(conn,
                 glue::glue_sql("DROP INDEX {`index_name`} ON {`to_schema`}.{`to_table`}", .con = conn))
    }
  }


  # LOAD DATA TO TABLE ----
  # Add message to user
  message(glue::glue("Loading to [{to_schema}].[{to_table}] from [{from_schema}].[{from_table}] table ", test_msg))

  # Run INSERT statement
  if (truncate_date == FALSE) {
    sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                SELECT {load_rows} * FROM {`from_schema`}.{`from_table`}",
                                  .con = conn,
                                  load_rows = DBI::SQL(load_rows))
  } else if (truncate_date == TRUE) {
    sql_combine <- glue::glue_sql(
      "INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
        SELECT {`archive_rows`} * FROM {`archive_schema`}.{`archive_table`}
          WHERE {`date_var`} < {date_cutpoint}
        UNION
        SELECT {load_rows} * FROM {`from_schema`}.{`from_table`}
        WHERE {`date_var`} >= {date_cutpoint}",
      .con = conn,
      archive_rows = DBI::SQL(archive_rows),
      load_rows = DBI::SQL(load_rows))
  }
  DBI::dbGetQuery(conn, sql_combine)
}
