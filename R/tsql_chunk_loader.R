# tsql_chunk_loader() ----
#' Loads large data sets to Microsoft SQL Server (TSQL) in 'chunks'
#'
#' @description
#' \code{tsql_chunk_loader} divides a data.frame/data.table into smaller tables
#' so it can be easily loaded into SQL. Experience has shown that loading large
#' tables in 'chunks' is less likely to cause errors. It is not needed for small
#' tables which load quickly. For **extremely large** datasets, you will likely
#' want to use the \href{https://learn.microsoft.com/en-us/sql/tools/bcp-utility?}{BCP
#' (Bulk Copy Program)}, which has been implemented in \code{\link[apde.etl]{load_df_bcp}}.
#'
#' @note Ported from the `rads` package. It has been migrated here in preparation for release of
#' `rads` version 2.0.0.
#'
#' @param ph.data The name of a single data.table/data.frame to be loaded to SQL Server
#' @param db_conn The name of the relevant open database connection to SQL Server
#' @param chunk_size The number of rows that you desire to have per upload 'chunk'
#' @param schema_name The name of the schema where you want to write the data
#' @param table_name The name of the table where you want to write the data
#' @param overwrite Do you want to overwrite an existing table? Logical (T|F).
#' Default `overwrite = FALSE`.
#' @param append Do you want to append to an existing table? Logical (T|F).
#' Default `append = TRUE`.
#' @param field_types *Optional!* A named character vector
#' with the desired TSQL datatypes for your upload. For example,
#' `c(col1 = 'int', col2 = 'float', col3 = 'date')`
#' @param validate_field_types Do you want to validate TSQL field types using
#' `rads::tsql_validate_field_types`? Logical (T|F).
#' Default `validate_field_types = TRUE`.
#' @param validate_upload Do you want to validate that all rows have been
#' uploaded? Logical (T|F).
#' Default `validate_upload = TRUE`.
#'
#' @details
#' `overwrite` & `append` are intentionally redundant in order to reduce the risk
#' of accidentally overwriting a table. Note that it is illogical for `overwrite`
#' & `append` to have the same value.
#'
#' The names in `field_types` must be the same as the names in `ph.data`. Also
#' note that `field_types` is only processed when `append = FALSE`. This prevents
#' conflicts with data types in pre-existing SQL tables.
#'
#' The `field_types` values are often derived from a yaml list, like those produced by [generate_yaml()].
#'
#' `validate_field_types = TRUE` is ignored if the `field_types` argument is not
#' provided.
#'
#' @seealso \code{\link[apde.etl]{load_df_bcp}} for a faster BCP-based approach
#' recommended for very large datasets where speed is critical.
#'
#' @name tsql_chunk_loader
#'
#' @examples
#' \donttest{
#'  library(data.table)
#'  mydt = data.table(col1 = 1:10000L,  # create integer
#'                    col2 = 1:10000/3) # create float
#'  mydt[, col3 := as.Date(Sys.Date()) - col1] # create date
#'  mydt[, col4 := as.character(col3)] # create string
#'  myfieldtypes <- c(col1 = 'int', col2 = 'float', col3 = 'date', col4 = 'nvarchar(255)')
#'
#'  tsql_chunk_loader(
#'    ph.data = mydt,
#'    db_conn = rads::validate_hhsaw_key(), # connect to Azure 16
#'    chunk_size = 3333,
#'    schema_name = Sys.getenv("USERNAME"),
#'    table_name = 'JustTesting',
#'    overwrite = TRUE,
#'    append = FALSE,
#'    field_types = myfieldtypes,
#'    validate_field_types = TRUE,
#'    validate_upload = TRUE
#'  )
#' }
#'
#' @export
#' @rdname tsql_chunk_loader
#'

tsql_chunk_loader <- function(ph.data = NULL, # R data.frame/data.table
                             db_conn = NULL, # connection name
                             chunk_size = 5000, # of rows of data to load at once
                             schema_name = NULL, # schema name
                             table_name = NULL, # table name
                             overwrite = FALSE, # overwrite?
                             append = TRUE, # append?
                             field_types = NULL,  # want to specify field types?
                             validate_field_types = TRUE, # validate specified field_types
                             validate_upload = TRUE){ # want to validate the upload?
  # Declare local variables used by data.table as NULL here to play nice with devtools::check() ----
    db_conn.name <- queryCount <- finalCount <- uploadedCount <- max.row.num <- NULL
    number.chunks <- starting.row <- ending.row <- NULL

  # Validate arguments ----
    # ph.data
        if(is.null(ph.data)){
          stop("\n\U1F6D1 You must specify a dataset (i.e., {ph.data} must be defined)")
        }
        if(!is.data.table(ph.data)){
          if(is.data.frame(ph.data)){
            setDT(ph.data)
          } else {
            stop(paste0("\n\U1F6D1 {ph.data} must be the name of a data.frame or data.table."))
          }
        }

    # db_conn
        if(is.null(db_conn)){stop('\n\U1F6D1 {db_conn} must be specified.')}
        if(class(db_conn)[1] != 'Microsoft SQL Server'){
          stop('\n\U1F6D1 {db_conn} is not a "Microsoft SQL Server" object.')}
        if(!DBI::dbIsValid(db_conn)){
          stop("\n\U1F6D1 {db_conn} must specify a valid database object. \nIf you are sure that it exists, confirm that it has not been disconnected.")
        }
        db_conn.name <- deparse(substitute(db_conn))

    # chunk_size
        if(chunk_size %% 1 != 0 | !chunk_size %between% c(100, 20000)){
          stop("\n\U1F6D1 {chunk_size} must be an integer between 100 and 20,000.")
        }

    # schema
        if(!is.character(schema_name) | length(schema_name) > 1){
          stop('\n\U1F6D1 {schema_name} must be a quoted name of a single schema, e.g., "ref", "claims", "death", etc.')
        }
        possible.schemas <- DBI::dbGetQuery(db_conn, "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA")[]$SCHEMA_NAME
        if(!schema_name %in% possible.schemas){
          stop(paste0('\n\U1F6D1 The value of {schema_name} (', schema_name, ') is not a valid schema name in {db_conn} (', db_conn.name, ').'))
        }

    # table_name
        if(!is.character(table_name) | length(table_name) > 1){
          stop('\n\U1F6D1 {table_name} must be a quoted name of a single table with your specified schema, e.g., "mytable1", "mytable2", etc.')
        }

    # overwrite
        if(!is.logical(overwrite)){
          stop('\n\U1F6D1 {overwrite} must be specified as a logical (i.e., TRUE, T, FALSE, or F)')
        }

    # append
        if(!is.logical(append)){
          stop('\n\U1F6D1 {append} must be specified as a logical (i.e., TRUE, T, FALSE, or F)')
        }
        if(overwrite == append){
          stop('\n\U1F6D1 {overwrite} & {append} cannot both be set to the same value! \nIf one is TRUE the other must be FALSE.')
        }

    # field_types
        if(append == TRUE){field_types = NULL}
        if(!is.null(field_types) && !identical(sort(names(field_types)), sort(names(ph.data))) ){
          stop('\n\U1F6D1 The names in {field_types} must match the column names in {ph.data}')
        }
        if(!is.null(field_types) && !(is.character(field_types) &&
             !is.null(names(field_types)) &&
             all(nzchar(names(field_types))))){
          stop('\n\U1F6D1 {field_types} is optional, but when provided must specify a named character vector. Please view the help file for details.')
        }

    # validate_upload
        if(!is.logical(validate_upload )){
          stop('\n\U1F6D1 {validate_upload } must be specified as a logical (i.e., TRUE, T, FALSE, or F)')
        }

    # validate_field_types
        if(!is.logical(validate_field_types )){
          stop('\n\U1F6D1 {validate_field_types } must be specified as a logical (i.e., TRUE, T, FALSE, or F)')
        }
        if(validate_field_types == TRUE & is.null(field_types)){
          validate_field_types = FALSE
        }

  # Validate field types if requested ----
        if(validate_field_types == TRUE){
          tsql_validate_field_types(ph.data = ph.data,
                                    field_types = field_types)
        }

  # Set initial values ----
    max.row.num <- nrow(ph.data)
    number.chunks <-  ceiling(max.row.num/chunk_size) # number of chunks to be uploaded
    starting.row <- 1 # the starting row number for each chunk to be uploaded. Initialize with 1
    ending.row <- chunk_size  # the final row number for each chunk to be uploaded. Initialize with overall chunk size

    if(validate_upload == TRUE){
      if(append == TRUE){
        querycnt <- sprintf("SELECT COUNT(*) as total_rows FROM %s.%s", schema_name, table_name)
        originalCount <- DBI::dbGetQuery(db_conn, querycnt)[]$total_rows
      } else { originalCount <- 0}
    }

  # Drop existing table if requested ----
    if(overwrite == TRUE & append == FALSE){
      DBI::dbGetQuery(conn = db_conn,
                      statement = paste0("IF OBJECT_ID('", schema_name, ".", table_name, "', 'U') IS NOT NULL ",
                                         "DROP TABLE ", schema_name, ".", table_name))
      overwrite = FALSE
      append = TRUE
    }

  # Create loop for appending new data ----
    for(i in 1:number.chunks){
      # counter so we know it is not stuck
        message(paste0(Sys.time(), ": Loading chunk ", format(i, big.mark = ','), " of ", format(number.chunks, big.mark = ','), ": rows ", format(starting.row, big.mark = ','), "-", format(ending.row, big.mark = ',')))

      # load the data chunk into SQL (will try each chunk up to 3 times)
        attempt <- 1 # initializing attempt counter
        while(attempt <= 3){
          tryCatch({
            # try to load to SQL
            if(is.null(field_types)){
              DBI::dbWriteTable(conn = db_conn,
                                name = DBI::Id(schema = schema_name, table = table_name),
                                value = ph.data[starting.row:ending.row,],
                                append = append,
                                row.names = FALSE)
            } else {
              DBI::dbWriteTable(conn = db_conn,
                                name = DBI::Id(schema = schema_name, table = table_name),
                                value = ph.data[starting.row:ending.row,],
                                append = FALSE, # set to false so can use field types
                                row.names = FALSE,
                                field.types = field_types)
              field_types = NULL # Reset field_types after use
            }

            # If operation succeeds, break out of the tryCatch loop
            break
          }, error = function(e){
            # If this was the third attempt, stop the process
            if(attempt == 3){
              stop(paste0('\n\U1F6D1 There have been three failed attempts to load chunk #',
                          format(i, big.mark = ','), '. \nThe script has stopped and you will have to ',
                          'try to correct the problem and run it again.') )
            } else {
              # Print an error message indicating a retry
              message(paste("Attempt", attempt, "failed. Trying again..."))
            }
          })

          # Increment the attempt counter for the next iteration
          attempt <- attempt + 1
        }

      # set the starting and ending rows for the next chunk to be uploaded
        starting.row <- starting.row + chunk_size
        ending.row <- min(starting.row + chunk_size - 1, max.row.num)
    }

  # Print summary of upload ----
    if(validate_upload == TRUE){
      # count rows in SQL
        queryCount <- sprintf("SELECT COUNT(*) as total_rows FROM %s.%s", schema_name, table_name)
        finalCount <- DBI::dbGetQuery(db_conn, queryCount)[]$total_rows

      # how many rows in SQL are new rows
        uploadedCount <- finalCount - originalCount

      # check if all rows of ph.data seem to have been uploaded
        if(uploadedCount == nrow(ph.data)){
          message("\U0001f642 \U0001f389 \U0001f38a \U0001f308 \n Congratulations! All the rows in {ph.data} were successfully uploaded.")
        } else {
          stop(paste0("\n\U1F6D1 \U2620 \U0001f47f \U1F6D1\n",
                      "{ph.data} has ", format(nrow(ph.data), big.mark = ','),
                      " rows, but [", schema_name, '].[', table_name, '] only has ',
                      format(uploadedCount, big.mark = ','), ' new rows.'))
        }
    }

}


