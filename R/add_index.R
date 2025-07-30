#' Add an index to a SQL table
#'
#' @description `add_index()` adds an index to a SQL table using specified variables or a YAML config file.
#'
#' @details This function adds a clustered column store (CCS) or clustered (CL)
#' index to a SQL table using specified variables or a YAML configuration file.
#' Users can specify some input functions (e.g., to_table) and rely on the config
#' file for the rest of the necessary information. For all arguments that could be
#' specified or come from a YAML file, the hierarchy is:
#' specified > argument under server in YAML > argument not under server in YAML.
#'
#' ## Example YAML file with no server and a CCS index
#' (Assume the indentation is appropriate)
#' ```yaml
#' to_schema: raw
#' to_table: mcaid_elig
#' # optional other components like a qa_schema and qa_table, variables, etc.
#' index_type: ccs
#' index_name: idx_ccs_raw_mcaid_elig
#' ```
#'
#' ## Example YAML file with servers (phclaims, hhsaw) and clustered index
#' (Assume the indentation is appropriate)
#' ```yaml
#' phclaims:
#'     to_schema: raw
#'     to_table: mcaid_elig
#' hhsaw:
#'     to_schema: raw
#'     to_table: mcaid_elig
#' # optional other components like a qa_schema and qa_table, variables, etc.
#' index_type: cl
#' index_name: idx_raw_mcaid_elig_id_date
#' index_vars:
#'     - id_apde
#'     - start_date
#' ```
#'
#' @note
#' This function replaces the deprecated `add_index` function from the
#' `apde` package.
#'
#' @param conn SQL server connection created using `odbc` package.
#' @param server Name of server being used (only applies if using a YAML file).
#' Useful if the same table is loaded to multiple servers but with different names
#' or schema.
#' @param config Name of object in global environment that contains configuration
#' information. Use one of `config`, `config_url`, or `config_file`.
#' <br><br>
#' Should be in a YAML format with at least the following variables: **to_schema**, **to_table**, and **index_name**.
#' <br><br>
#' The `config` *may* also contain **index_type** and **index_vars** variables.
#' <br><br>
#' **to_schema** and **to_table** should be nested under the server name if applicable,
#' other variables should not.
#' <br><br>
#' If **index_type** is not specified and **index_vars**
#' fields are named, the assumption is a clustered (CL) index.
#' @param config_url URL of a YAML config file. Use one of `config`, `config_url`, or
#' `config_file`. Note the requirements under `config`.
#' @param config_file File path of a YAML config file. Use one of `config`, `config_url`, or
#' `config_file`. Note the requirements under `config`.
#' @param to_schema Name of the schema to apply the index to (if not using YAML input).
#' @param to_table Name of the table to apply the index to (if not using YAML input).
#' @param index_type Which index type will be used, either 'ccs' or 'cl' (if not using YAML input).
#' @param index_name Name of the index to be added (if not using YAML input).
#' @param index_vars Vector of variables to index on if using a clustered ('cl') index (if not using YAML input).
#' Should take the format `c("a", "b", "c")`.
#' @param drop_index Remove any existing clustered or clustered column store indices.
#' Default is `TRUE`.
#' @param test_schema Add index to a temporary/development schema when testing out table creation.
#' Will use the to_schema (specified or in the YAML file) to make a new table name of
#' `{to_schema}_{to_table}`. Schema must already exist in the database. Most useful
#' when the user has an existing YAML file and does not want to overwrite it. Default is `NULL`.
#'
#' @return No return value. Function is called for its side effects of adding an index to a SQL table.
#'
#' @examples
#' \dontrun{
#' add_index(conn = db_claims, server = "hhsaw", config = load_config)
#' }
#'
#' @export

add_index <- function(conn,
                      server = NULL,
                      config = NULL,
                      config_url = NULL,
                      config_file = NULL,
                      to_schema = NULL,
                      to_table = NULL,
                      index_type = NULL,
                      index_name = NULL,
                      index_vars = NULL,
                      drop_index = T,
                      test_schema = NULL) {

  # INITIAL ERROR CHECKS ----
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

  ## index_name ----
  if (is.null(index_name)) {
    if (!is.null(table_config$index_name)) {
      index_name <- table_config$index_name
    } else {
      stop("index_name must be specified or present in the config file.")
    }
  }

  ## index_type ----
  if (!is.null(index_type)) {
    if (!index_type %in% c("ccs", "cl")) {
      stop("Unknown index_type specified, choose from 'ccs' or 'cl'")
    }
  } else if (!is.null(table_config$index_type)) {
    if (!table_config$index_type %in% c("ccs", "cl")) {
      stop("Unknown index_type specified in YAML file, choose from 'ccs' or 'cl'")
    } else {
      index_type <- table_config$index_type
    }
  } else {
    stop("Unknown index_type specified, choose from 'ccs' or 'cl'")
  }

  ## index_vars ----
  # Make sure there are fields to index on if using a clustered index
  if (index_type == "cl") {
    if (is.null(index_vars) & is.null(table_config$index_vars)) {
      stop("A clustered index was chosen but no variables to index on are present. Cannot proceed.")
    } else if (is.null(index_vars) & !is.null(table_config$index_vars)) {
      index_vars <- table_config$index_vars
    }
  }


  # TEST MODE ----
  # Alert users they are in test mode
  if (!is.null(test_schema)) {
    message("FUNCTION WILL BE RUN IN TEST MODE, INDEXING TABLE IN ", toupper(test_schema), " SCHEMA")
    to_table <- glue::glue("{to_schema}_{to_table}")
    to_schema <- test_schema
  }


  # REMOVE EXISTING INDICES IF DESIRED ----
  if (drop_index == T) {
    # This code pulls out the index name
    existing_index <- DBI::dbGetQuery(conn,
                                      glue::glue_sql("SELECT DISTINCT a.index_name
                     FROM
                     (SELECT ind.name AS index_name
                       FROM
                       (SELECT object_id, name, type_desc FROM sys.indexes
                         WHERE type_desc LIKE 'CLUSTERED%') ind
                       INNER JOIN
                       (SELECT name, schema_id, object_id FROM sys.tables
                         WHERE name = {to_table}) t
                       ON ind.object_id = t.object_id
                       INNER JOIN
                       (SELECT name, schema_id FROM sys.schemas
                         WHERE name = {to_schema}) s
                       ON t.schema_id = s.schema_id) a",
                                                     .con = conn))

    if (nrow(existing_index) != 0) {
      message("Removing existing clustered/clustered columnstore index/indices")
      lapply(seq_along(existing_index), function(i) {
        DBI::dbExecute(conn,
                       glue::glue_sql("DROP INDEX {`existing_index[['index_name']][[i]]`}
                                        ON {`to_schema`}.{`to_table`}",
                                      .con = conn))
      })
    }
  }


  # ADD INDEX ----
  message(glue::glue("Adding index ({index_name}) to {to_schema}.{to_table}"))

  if (index_type == 'ccs') {
    # Clustered column store index
    DBI::dbExecute(conn,
                   glue::glue_sql("CREATE CLUSTERED COLUMNSTORE INDEX {`index_name`} ON
                   {`to_schema`}.{`to_table`}",
                                  .con = conn))
  } else {
    # Clustered index
    DBI::dbExecute(conn,
                   glue::glue_sql("CREATE CLUSTERED INDEX {`index_name`} ON
                   {`to_schema`}.{`to_table`}({`index_vars`*})",
                                  .con = conn))
  }

}
