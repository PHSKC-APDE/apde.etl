#' @title Create a SQL table
#'
#' @description
#' `create_table()` creates a SQL table using specified variables or a YAML config file.
#'
#' @note
#' This function replaces the deprecated `create_table` function from the
#' `apde` package.
#'
#' @details This function creates tables in a SQL database using specified variables or a YAML configuration file.
#' Users can specify some input functions (e.g., to_table) and rely on the config
#' file for the rest of the necessary information. For all arguments that could be
#' specified or come from a YAML file, the hierarchy is
#' specified > argument under server in YAML > argument not under server in YAML.
#'
#' ## Performance Notes
#' When creating multiple year tables (`ind_yr = TRUE`), tables are created sequentially.
#' Large numbers of years may take significant time depending on database performance.
#'
#' ## Example YAML file with no server or individual years
#' (Assume the indentation is appropriate)
#' ```yaml
#' to_schema: raw
#' to_table: mcaid_elig
#' # optional other components like a qa_schema and qa_table, file path to data, index name, etc.
#' vars:
#'     CLNDR_YEAR_MNTH: INTEGER
#'     MEDICAID_RECIPIENT_ID: VARCHAR(255)
#'     RPRTBL_RAC_CODE: INTEGER
#'     RPRTBL_RAC_NAME: VARCHAR(255)
#'     RPRTBL_BSP_GROUP_CID: INTEGER
#'     RPRTBL_BSP_GROUP_NAME: VARCHAR(255)
#'     FROM_DATE: DATE
#'     TO_DATE: DATE
#' ```
#'
#' ## Example YAML file with servers (phclaims, hhsaw) and individual years
#' (Assume the indentation is appropriate)
#' ```yaml
#' phclaims:
#'     to_schema: raw
#'     to_table: mcaid_elig
#' hhsaw:
#'     to_schema: raw
#'     to_table: mciad_elig
#' # optional other components like a qa_schema and qa_table, file path to data, index name, etc.
#' vars:
#'     CLNDR_YEAR_MNTH: INTEGER
#'     MEDICAID_RECIPIENT_ID: VARCHAR(255)
#'     RPRTBL_RAC_CODE: INTEGER
#'     RPRTBL_RAC_NAME: VARCHAR(255)
#'     RPRTBL_BSP_GROUP_CID: INTEGER
#'     RPRTBL_BSP_GROUP_NAME: VARCHAR(255)
#'     FROM_DATE: DATE
#'     TO_DATE: DATE
#' years:
#'     - 2014
#'     - 2015
#'     - 2016
#' 2014:
#'     vars:
#'         DUAL_ELIG: VARCHAR(255)
#'         TPL_FULL_FLAG: VARCHAR(255)
#' 2016:
#'     vars:
#'         SECONDARY_RAC_CODE: INTEGER
#'         SECONDARY_RAC_NAME: VARCHAR(255)
#' ```
#'
#' @param conn SQL server connection created using `odbc` package.
#' @param server Name of server being used (only applies if using a YAML file).
#' Useful if the same table is loaded to multiple servers but with different names
#' or schema.
#' @param config Name of object in global environment that contains configuration
#' information. Use one of `config`, `config_url`, or `config_file`.
#' Should be in a YAML format with at least the following variables:
#' **to_schema**, **to_table**, **vars**. **to_schema** and **to_table** should be
#' nested under the server name if applicable. The **vars** variable should not be listed
#' under the server name but should list all variables in the table along with
#' data type, e.g., id_apde VARCHAR(20). If a server is being specified,
#' the **to_schema** and **to_table** variables should be nested under that server name.
#' @param config_url URL of a YAML config file. Use one of `config`, `config_url`, or
#' `config_file`. Note the requirements under `config`.
#' @param config_file File path of a YAML config file. Use one of `config`, `config_url`, or
#' `config_file`. Note the requirements under `config`.
#' @param to_schema Name of the schema to apply the index to (if not using YAML input).
#' @param to_table Name of the table to apply the index to (if not using YAML input).
#' @param vars Named vector of variables to create in the table (if not using YAML input).
#' Should take the format `c("a" = "VARCHAR(255)", "b" = "DATE", "c" = "INTEGER")`.
#' @param overwrite Drop table first before creating it, if it exists. Default is TRUE.
#' @param external Create external table. This option requires specifying data source
#' details in the **ext_data_source**, **ext_schema**, and **ext_object_name** variables
#' (either when calling the function or in the YAML file). If in the YAML file, these
#' variables should not be nested under a server name. Default is FALSE.
#' @param ext_data_source Name of the external data source (if not using YAML input).
#' This pointer should already be established in the SQL database.
#' @param ext_schema Name of the external data schema (if not using YAML input).
#' @param ext_object_name Name of the external data table (if not using YAML input).
#' @param overall Create single table instead of a table for each calendar year.
#' Mutually exclusive with **ind_yr** option. Default is TRUE.
#' @param ind_yr Create multiple tables with the same core structure, one for each
#' calendar year, with a year suffix on each table name (e.g., mcaid_elig_2014).
#' Mutually exclusive with **overall** option. If using this option, the list of years
#' should be provided via the **years** argument or a **years** variable in the YAML file.
#' If a given year has additional specific fields, these should be listed in the **vars** variable,
#' nested under the calendar year (see example in description). Default is FALSE.
#' @param years Vector of individual years to make tables for (if not using YAML input).
#' @param years_vars List of named vectors of additional variables that are specific to
#' a given year (if not using YAML input). Should take the format
#' `list("2014" = c("DUAL_ELIG" = "VARCHAR(255)", "TPL_FULL_FLAG" = "VARCHAR(255)"),
#' "2016" = c("SECONDARY_RAC_CODE" = "INTEGER", "SECONDARY_RAC_NAME" = "VARCHAR(255)"))`
#' where the list names match the years to load.
#' @param test_schema Use a temporary/development schema to test out table creation.
#' Will use the to_schema (specified or in the YAML file) to make a new table name of
#' `{to_schema}_{to_table}`. Schema must already exist in the database. Most useful
#' when the user has an existing YAML file and does not want to overwrite it. Default is NULL.
#'
#' @return No return value, called for side effects (creates SQL tables).
#'
#' @examples
#' \dontrun{
#' create_table(conn = db_claims, server = "hhsaw", config = load_config)
#' create_table(conn = db_claims, server = "phclaims",
#' config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/load_mcaid_raw.R",
#' overall = FALSE, ind_yr = TRUE)
#' }
#'
#' @keywords database
#' @export

create_table <- function(conn,
                         server = NULL,
                         config = NULL,
                         config_url = NULL,
                         config_file = NULL,
                         to_schema = NULL,
                         to_table = NULL,
                         vars = NULL,
                         overwrite = T,
                         external = F,
                         ext_data_source = NULL,
                         ext_schema = NULL,
                         ext_object_name = NULL,
                         overall = T,
                         ind_yr = F,
                         years = NULL,
                         years_vars = NULL,
                         test_schema = NULL) {

  # INITIAL ERROR CHECKS ----
  # Check if the config provided is a local file or on a webpage
  if (!is.null(config) & !is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a local config object, config_url, or config_file but only one")
  }

  if (!is.null(config_file)) {
    # Check that the yaml config file exists in the right format
    if (file.exists(config_file) == F) {
      stop("Config file does not exist, check file name")
    }

    if (configr::is.yaml.file(config_file) == F) {
      stop(glue::glue("Config file is not a YAML config file. ",
                      "Check there are no duplicate variables listed"))
    }
  }

  # Check that something will be run (but not both things)
  if (overall == F & ind_yr == F) {
    stop("At least one of 'overall and 'ind_yr' must be set to TRUE")
  }

  if (overall == T & ind_yr == T) {
    stop("Only one of 'overall and 'ind_yr' can be set to TRUE")
  }


  # READ IN CONFIG FILE ----
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(httr::GET(config_url))
  } else if (!is.null(config_file)) {
    table_config <- yaml::read_yaml(config_file)
  } else {
    table_config <- NULL
  }

  # Make sure a valid URL was found
  if ('404' %in% names(table_config)) {
    stop("Invalid URL for YAML file")
  }


  # VARIABLES ----
  ## to_schema ----
  if (is.null(to_schema)) {
    if (!is.null(server)) {
      if (!is.null(table_config[[server]][["to_schema"]])) {
        to_schema <- table_config[[server]][["to_schema"]]
      } else if (!is.null(table_config$to_schema)) {
        to_schema <- table_config$to_schema
      }
    } else if (!is.null(table_config$to_schema)) {
      to_schema <- table_config$to_schema
    }
  }

  ## to_table ----
  if (is.null(to_table)) {
    if (!is.null(server)) {
      if (!is.null(table_config[[server]][["to_table"]])) {
        to_table <- table_config[[server]][["to_table"]]
      } else if (!is.null(table_config$to_table)) {
        to_table <- table_config$to_table
      }
    } else if (!is.null(table_config$to_table)) {
      to_table <- table_config$to_table
    }
  }


  ## vars ----
  if (is.null(vars)) {
    vars <- table_config$vars
  }

  # Make sure there are no duplicate fields
  if (length(names(vars)) != length(unique(names(vars)))) {
    stop("There are duplicate fields names present. Check vars input/YAML file and try again.")
  }


  # TEST MODE ----
  # Alert users they are in test mode
  if (!is.null(test_schema)) {
    message("FUNCTION WILL BE RUN IN TEST MODE, WRITING TO ", toupper(test_schema), " SCHEMA")
    test_msg <- " (function is in test mode)"
    to_table <- glue::glue("{to_schema}_{to_table}")
    to_schema <- test_schema
  } else {
    test_msg <- ""
  }


  # EXTERNAL TABLE ----
  if (external == T) {
    if (is.null(ext_data_source)) {
      ext_data_source <- table_config$ext_data_source
    }
    if (is.null(ext_schema)) {
      ext_schema <- table_config$ext_schema
    }
    if (is.null(ext_object_name)) {
      ext_object_name <- table_config$ext_object_name
    }

    external_setup <- glue::glue_sql(" EXTERNAL ", .con = conn)
    external_text <- glue::glue_sql(" WITH (DATA_SOURCE = {DBI::SQL(ext_data_source)},
                                    SCHEMA_NAME = {ext_schema},
                                    OBJECT_NAME = {ext_object_name})", .con = conn)
  } else {
    external_setup <- DBI::SQL("")
    external_text <- DBI::SQL("")
  }


  # OVERALL TABLE ----
  if (overall == T) {
    message(glue::glue("Creating overall [{to_schema}].[{to_table}] table", test_msg))

    if (overwrite == T) {
      if (DBI::dbExistsTable(conn, DBI::Id( schema = to_schema, table = to_table))) {
        DBI::dbExecute(conn,
                       glue::glue_sql("DROP {external_setup} TABLE {`to_schema`}.{`to_table`}",
                                      .con = conn))
      }
    }


    create_code <- glue::glue_sql(
      "CREATE {external_setup} TABLE {`to_schema`}.{`to_table`} (
      {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(vars)`} {DBI::SQL(vars)}',
      .con = conn), sep = ', \n'))}
      ) {external_text}",
      .con = conn)

    DBI::dbExecute(conn, create_code)
  }


  # CALENDAR YEAR TABLES ----
  if (ind_yr == T) {
    # Use unique in case years are repeated
    if (!is.null(years)) {
      years <- sort(unique(years))
    } else {
      years <- sort(unique(table_config$years))
    }

    message(glue::glue("Creating calendar year [{to_schema}].[{to_table}] tables", test_msg))

    lapply(years, function(x) {
      # Set up new table name
      to_table <- paste0(to_table, "_", x)

      # Add additional year-specific variables if present
      if (!is.null(years_vars)) {
        vars <- c(vars, years_vars[[x]])
      } else if ("vars" %in% names(table_config[[x]])) {
        vars <- c(vars, table_config[[x]][["vars"]])
      }
      # Remove any duplicates
      vars <- vars[unique(names(vars))]


      if (overwrite == T) {
        if (DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
          DBI::dbExecute(conn,
                         glue::glue_sql("DROP {external_setup} TABLE {`to_schema`}.{`to_table`}",
                                        .con = conn))
        }
      }

      create_code <- glue::glue_sql(
        "CREATE {external_setup} TABLE {`to_schema`}.{`to_table`} (
      {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(vars)`} {DBI::SQL(vars)}',
      .con = conn), sep = ', \n'))}
      ) {external_text}",
        .con = conn)

      DBI::dbExecute(conn, create_code)
    })
  }
}
