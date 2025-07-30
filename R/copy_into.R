#' @title Copy Data from the Data Lake to the Data Warehouse
#'
#' @description
#' This function copies data from the data lake to the data warehouse using SQL Server's COPY INTO statement.
#'
#' @author Alastair Matheson, 2019-04-04
#'
#' @details
#' Refactored on 7/25/2025 by Danny colombara to improve modularity and maintainability
#' by extracting helper functions and improving code organization.
#'
#' Plans for future improvements:
#' - Add warning when table is about to be overwritten.
#' - Add other options for things we're not using (e.g., file_format).
#'
#' @note
#' This function replaces the deprecated `copy_into_f` function from the
#' `apde` package. The functionality is identical, but this version has
#' improved code organization and maintainability.
#'
#' @param conn SQL server connection created using `odbc` package
#' @param server Server name: 'hhsaw' or 'phclaims'. If NULL, will be set to NA.
#' @param config A list object in memory containing the YAML config file contents (should be NULL if using config_url or config_file)
#' @param config_url The URL location of the YAML config file (should be NULL if using config or config_file)
#' @param config_file The file path of the YAML config file (should be NULL if using config or config_url)
#' @param to_schema Target schema name in the data warehouse
#' @param to_table Target table name in the data warehouse
#' @param db_name Database name (e.g., "hhs_analytics_workspace", "inthealth_edw")
#' @param dl_path The data lake path where the source files are located
#' @param file_type File type: `"csv"`, `"parquet"`, or `"orc"`
#' @param identity The identity (username or account name) for data lake authentication
#' @param secret The secret key or password for data lake authentication
#' @param max_errors Maximum number of records that can be rejected before the entire file is rejected (default: `100`)
#' @param compression Compression type: `"none"`, `"gzip"`, `"defaultcodec"`, or `"snappy"` (default: `"none"`)
#' @param field_quote Character used to quote fields in the input file (default: empty string)
#' @param field_term Character or string used to separate fields in the input file
#' @param row_term Character or string used to separate rows in the input file
#' @param first_row Row number where data begins in the input file, excluding headers (default: `2`)
#' @param overwrite Logical; if TRUE, truncate the table before creating it (default: `TRUE`)
#' @param rodbc Logical; if TRUE, use RODBC package to avoid encoding errors with secret keys (default: `FALSE`)
#' @param rodbc_dsn DSN name for the RODBC connection (default: `"int_edw_16"`)
#'
#'
#' @return None (invisible NULL)
#'
#' @export
#'

#### MAIN FUNCTION ####
copy_into <- function(conn,
                      server = NULL,
                      config = NULL,
                      config_url = NULL,
                      config_file = NULL,
                      to_schema = NULL,
                      to_table = NULL,
                      db_name = NULL,
                      dl_path = NULL,
                      file_type = c("csv", "parquet", "orc"),
                      identity = NULL,
                      secret = NULL,
                      max_errors = 100,
                      compression = c("none", "gzip", "defaultcodec", "snappy"),
                      field_quote = "",
                      field_term = NULL,
                      row_term = NULL,
                      first_row = 2,
                      overwrite = TRUE,
                      rodbc = FALSE,
                      rodbc_dsn = "int_edw_16") {

  # All helper functions are below the main function's code ----

  # INPUT VALIDATION ----
  server <- validate_server(server)
  validate_config_inputs(config, config_url, config_file)
  validate_numeric_params(max_errors, first_row)

  # PARAMETER PROCESSING ----
  file_type <- match.arg(file_type)
  compression <- match.arg(compression)
  max_errors <- round(max_errors, 0)
  first_row <- round(first_row, 0)

  # LOAD AND VALIDATE CONFIG ----
  table_config <- load_table_config(config, config_url, config_file)
  validate_table_config(table_config)

  # EXTRACT PARAMETERS FROM CONFIG ----
  to_schema <- get_config_param(to_schema, table_config, server, "to_schema")
  to_table <- get_config_param(to_table, table_config, server, "to_table")
  dl_path <- get_config_param(dl_path, table_config, server, "dl_path")
  db_name <- get_config_param(db_name, table_config, server, "db_name")

  # PREPARE SQL COMPONENTS ----
  if (compression == "none") {
    compression <- DBI::SQL("")
  }

  auth_sql <- create_auth_sql(rodbc, identity, secret, conn)

  # TABLE CREATION ----
  handle_table_creation(conn, to_schema, to_table, overwrite, table_config)
  message(glue::glue("Creating [{to_schema}].[{to_table}] table"))

  # EXECUTE COPY INTO ----
  load_sql <- glue::glue_sql(
    "COPY INTO {`to_schema`}.{`to_table`}
    ({`names(table_config$vars)`*})
    FROM {dl_path}
    WITH (
      FILE_TYPE = {file_type},
      {auth_sql}
      MAXERRORS = {max_errors},
      COMPRESSION = {compression},
      FIELDQUOTE = {field_quote},
      FIELDTERMINATOR = {field_term},
      ROWTERMINATOR = {row_term},
      FIRSTROW = {first_row}
    );",
    .con = conn
  )

  execute_copy_into(conn, rodbc, rodbc_dsn, load_sql)

  return(invisible(NULL))
}

#### HELPER FUNCTIONS ####
#' HELPER: Validate server parameter
#' @param server Server name to validate
#' @return Validated server name or NA
validate_server <- function(server) {
  if (is.null(server)) {
    return(NA)
  }

  valid_servers <- c("phclaims", "hhsaw")
  if (!server %in% valid_servers) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }

  return(server)
}

#' HELPER: Validate configuration inputs
#' @param config Local config object
#' @param config_url Config URL
#' @param config_file Config file path
validate_config_inputs <- function(config, config_url, config_file) {
  config_count <- sum(!is.null(config), !is.null(config_url), !is.null(config_file))

  if (config_count != 1) {
    stop("Specify exactly one of: config, config_url, or config_file")
  }

  if (!is.null(config_url)) {
    message("Warning: YAML configs pulled from a URL are subject to fewer error checks")
  }

  if (!is.null(config_file)) {
    validate_config_file(config_file)
  }
}

#' HELPER: Validate configuration file
#' @param config_file Path to configuration file
validate_config_file <- function(config_file) {
  if (!file.exists(config_file)) {
    stop("Config file does not exist, check file name")
  }

  if (!configr::is.yaml.file(config_file)) {
    stop(glue::glue("Config file is not a YAML config file. ",
                    "Check there are no duplicate variables listed"))
  }
}

#' HELPER: Load configuration from various sources
#' @param config Local config object
#' @param config_url Config URL
#' @param config_file Config file path
#' @return Loaded configuration object
load_table_config <- function(config, config_url, config_file) {
  if (!is.null(config)) {
    return(config)
  } else if (!is.null(config_url)) {
    return(yaml::yaml.load(httr::GET(config_url)))
  } else {
    return(yaml::read_yaml(config_file))
  }
}

#' HELPER: Validate loaded configuration
#' @param table_config Loaded configuration object
validate_table_config <- function(table_config) {
  if ("404" %in% names(table_config)) {
    stop("Invalid URL for YAML file")
  }

  if (!"vars" %in% names(table_config)) {
    stop("YAML file is missing a list of variables")
  }

  if (is.null(table_config$vars)) {
    stop("No variables specified in config file")
  }
}

#' HELPER: Validate numeric parameters
#' @param max_errors Maximum errors parameter
#' @param first_row First row parameter
validate_numeric_params <- function(max_errors, first_row) {
  if (!is.numeric(max_errors)) {
    stop("max_errors must be numeric")
  }

  if (!is.numeric(first_row)) {
    stop("first_row must be numeric")
  }
}

#' HELPER: Get parameter value from config or direct input
#' @param direct_value Direct parameter value
#' @param table_config Configuration object
#' @param server Server name
#' @param param_name Parameter name in config
#' @return Parameter value
get_config_param <- function(direct_value, table_config, server, param_name) {
  if (!is.null(direct_value)) {
    return(direct_value)
  }

  # Try server-specific config first, then general config
  server_config <- table_config[[server]][[param_name]]
  if (!is.null(server_config)) {
    return(server_config)
  }

  general_config <- table_config[[param_name]]
  if (!is.null(general_config)) {
    return(general_config)
  }

  return(NULL)
}

#' HELPER: Create authentication SQL fragment
#' @param rodbc Whether using RODBC
#' @param identity Authentication identity
#' @param secret Authentication secret
#' @param conn Database connection
#' @return SQL fragment for authentication
create_auth_sql <- function(rodbc, identity, secret, conn) {
  if (rodbc && (!is.null(identity) || !is.null(secret))) {
    return(glue::glue_sql("CREDENTIAL = (IDENTITY = {identity}, SECRET = {secret}),", .con = conn))
  }
  return(DBI::SQL(""))
}

#' HELPER: Handle table creation and overwrite logic
#' @param conn Database connection
#' @param to_schema Target schema
#' @param to_table Target table
#' @param overwrite Whether to overwrite existing table
#' @param table_config Configuration object
handle_table_creation <- function(conn, to_schema, to_table, overwrite, table_config) {
  table_id <- DBI::Id(schema = to_schema, table = to_table)

  if (overwrite) {
    message("Removing existing table and creating new one")
    if (DBI::dbExistsTable(conn, table_id)) {
      DBI::dbExecute(conn, glue::glue_sql("DROP TABLE {`to_schema`}.{`to_table`}", .con = conn))
    }
  }

  if (!DBI::dbExistsTable(conn, table_id)) {
    create_table_sql <- glue::glue_sql(
      "CREATE TABLE {`to_schema`}.{`to_table`} (
          {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(table_config$vars)`} {DBI::SQL(table_config$vars)}',
                                           .con = conn), sep = ', \n'))}
        )",
      .con = conn
    )
    DBI::dbExecute(conn, create_table_sql)
  }
}

#' HELPER: Execute the COPY INTO statement
#' @param conn Database connection
#' @param rodbc Whether to use RODBC
#' @param rodbc_dsn RODBC DSN name
#' @param load_sql SQL statement to execute
execute_copy_into <- function(conn, rodbc, rodbc_dsn, load_sql) {
  # TEMPORARY FIX FOR ODBC ISSUES:
  # The odbc package isn't encoding the secret key properly right now so produces
  # a Base-64 error. The RODBC doesn't seem to have that issue so for now we are
  # forcing the COPY INTO statement to use an RODBC connection
  if (rodbc) {
    conn_rodbc <- RODBC::odbcConnect(
      dsn = rodbc_dsn,
      uid = keyring::key_list("hhsaw_dev")[["username"]]
    )
    RODBC::sqlQuery(channel = conn_rodbc, query = load_sql)
    RODBC::odbcClose(conn_rodbc)
  } else {
    DBI::dbExecute(conn, load_sql)
  }
}

