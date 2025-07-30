#' @title Compare external table to source table to find changes
#'
#' @description Performs schema validation between source and external tables by
#' querying INFORMATION_SCHEMA metadata. When inconsistencies are detected,
#' constructs complete external table recreation script including proper data
#' source references.
#'
#' @note
#' This function replaces the deprecated `external_table_check_f` function from the
#' `apde` package.
#'
#' @param conn SQL server connection created using `odbc` package
#' @param db name of database/data warehouse for source table, must be inthealth_edw or inthealth_dwhealth
#' @param schema name of schema for source table
#' @param table name of source table
#' @param conn_ext name of the connection to the SQL database with external table
#' @param db_ext name of database/data warehouse for external table
#' @param schema_ext name of schema for external table
#' @param table_ext name of external table
#' @param sql_display show the SQL script in Console
#' @param sql_file_path write the SQL script to file
#' @param overwrite overwrite sql script, appends if FALSE
#'
#' @return Logical. Returns TRUE if source and external table schemas match exactly,
#' FALSE if they differ. Also displays/writes SQL script for external table
#' recreation when schemas don't match.
#'
#' @export
#'

#### FUNCTION ####
external_table_check <- function(conn,
                                   db = c("inthealth_edw", "inthealth_dwhealth"),
                                   schema,
                                   table,
                                   db_ext = "hhs_analytics_workspace",
                                   conn_ext,
                                   schema_ext,
                                   table_ext,
                                   sql_display = TRUE,
                                   sql_file_path = NULL,
                                   overwrite = TRUE) {

  # Variables
  db <- match.arg(db)

  # Get source column information
  source_cols <- DBI::dbGetQuery(conn,
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
                                                  		WHEN [DATA_TYPE] IN('VARCHAR', 'CHAR', 'NVARCHAR') THEN CONCAT('(',[CHARACTER_MAXIMUM_LENGTH], ') COLLATE ', [COLLATION_NAME])
                                                  		WHEN [DATA_TYPE] IN('DECIMAL', 'NUMERIC') THEN CONCAT('(', [NUMERIC_PRECISION], ',', [NUMERIC_SCALE], ')')
                                                  		ELSE ''
                                                  	END,
	                                                  ' NULL') AS 'COLUMN_DEFINITION'
                                                FROM [INFORMATION_SCHEMA].[COLUMNS]
                                                WHERE
                                                  [TABLE_NAME] = {table}
                                                  AND [TABLE_SCHEMA] = {schema}
                                                ORDER BY [ORDINAL_POSITION]",
                                                .con = conn))

  if(nrow(source_cols) == 0) {
    stop(glue::glue("Error: The Source Table [{schema}].[{table}] Does NOT Exist!"))
  }

  # Get current external column information (blank if none exisits)
  external_cols <- DBI::dbGetQuery(conn_ext,
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
                                                  		WHEN [DATA_TYPE] IN('VARCHAR', 'CHAR', 'NVARCHAR') THEN CONCAT('(',[CHARACTER_MAXIMUM_LENGTH], ') COLLATE ', [COLLATION_NAME])
                                                  		WHEN [DATA_TYPE] IN('DECIMAL', 'NUMERIC') THEN CONCAT('(', [NUMERIC_PRECISION], ',', [NUMERIC_SCALE], ')')
                                                  		ELSE ''
                                                  	END,
	                                                  ' NULL') AS 'COLUMN_DEFINITION'
                                                FROM [INFORMATION_SCHEMA].[COLUMNS]
                                                WHERE
                                                  [TABLE_NAME] = {table_ext}
                                                  AND [TABLE_SCHEMA] = {schema_ext}
                                                ORDER BY [ORDINAL_POSITION]",
                                                .con = conn_ext))
  # Compare all columns, column types, lengths, precision, etc.
  result <- all.equal(source_cols, external_cols)

  # If everything matches, end function and return TRUE
  if(result == TRUE) {
    message(glue::glue("Source Table [{schema}].[{table}] Matches External Table [{schema_ext}].[{table_ext}]"))
    return(TRUE)
  }




  # Sets datasource depending on source database
  if(db == "inthealth_edw") {
    data_source <- "datascr_WS_EDW"
  } else {
    data_source <- "datasrc_WS_IntHealth"
  }

  # Create SQL script
  sql <- glue::glue_sql("
IF OBJECT_ID('{`schema_ext`}.{`table_ext`}') IS NOT NULL
  DROP EXTERNAL TABLE {`schema_ext`}.{`table_ext`};
CREATE EXTERNAL TABLE {`schema_ext`}.{`table_ext`}
  ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{DBI::SQL(source_cols$COLUMN_DEFINITION)}',.con = conn_ext), sep = ', \n  '))})
WITH (DATA_SOURCE = [{DBI::SQL(data_source)}], SCHEMA_NAME = N{schema}, OBJECT_NAME = N{table});", .con = conn_ext)

  # Display SQL script in console
  if(sql_display == TRUE) {
    message(glue::glue("External Table Creation SQL Script for [{schema_ext}].[{table_ext}]:"))
    message(sql)
  }

  # Write SQL Script to file
  if(is.null(sql_file_path) == FALSE) {
    # If file exists and overwrite is FALSE, append the script to the file.
    # Else, create a new file/overwrite the existing file
    if(file.exists(sql_file_path) == TRUE && overwrite == FALSE) {
      message(glue::glue("Appending SQL Script for [{schema_ext}].[{table_ext}] to: {sql_file_path}"))
      write(paste0("\n", sql), file = sql_file_path, append = TRUE)
    } else {
      message(glue::glue("Writing SQL Script for [{schema_ext}].[{table_ext}] to: {sql_file_path}"))
      write(sql, file = sql_file_path, append = FALSE)
    }
  }

  # If anything did not match, end function and return FALSE
  return(FALSE)
}
