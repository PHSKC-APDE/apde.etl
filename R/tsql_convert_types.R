# tsql_convert_types() ----
#' Converts R data types to match TSQL field types using lossless conversion
#'
#' @description
#' `tsql_convert_types` attempts to safely convert columns in a
#' data.table/data.frame to match specified TSQL field types using the
#' [rads::lossless_convert()] function. The function will only perform conversions
#' that do not introduce new missing values or lose information. This is
#' particularly useful when preparing data for upload to Microsoft SQL Server
#' where R data types don't perfectly align with expected TSQL types (e.g.,
#' character columns that contain numeric values but need to be integers).
#'
#' @note Ported from the `rads` package. It has been migrated here in preparation for release of
#' `rads` version 2.0.0.
#'
#' @param ph.data The name of a single data.table/data.frame to be converted.
#' @param field_types A named character vector with the desired TSQL datatypes
#'   for your conversion. For example, `c(col1 = 'int', col2 = 'float',
#'   col3 = 'date')`. Note that the names in `field_types` must be the
#'   same as the names in `ph.data`. This is often read into memory from a
#'   *.yaml file, but can also be manually created.
#' @param validate_before Logical. Should [tsql_validate_field_types()] be
#'   run before attempting conversion? If `TRUE` and validation passes, no
#'   conversion will be attempted. Default is `TRUE`.
#' @param validate_after Logical. Should [tsql_validate_field_types()] be
#'   run after conversion to confirm success? Default is `TRUE`.
#' @param verbose Logical. Should detailed conversion messages be displayed?
#'   Default is `TRUE`.
#' @param return_log Logical. Should conversion log be returned along with
#'   converted data? If `TRUE`, returns a list with 'data' and
#'   'conversion_log' elements. Default is `FALSE`.
#'
#' @name tsql_convert_types
#'
#' @details This function uses [rads::lossless_convert()] to safely attempt type
#' conversions. Conversions that would introduce new `NA` values or lose
#' information will not be performed, and the original column will be retained.
#'
#' The function maps TSQL types to appropriate R classes as follows:
#' - `tinyint`, `smallint`, `int`, `bit` → `integer`
#' - `bigint`, `decimal`, `numeric`, `float`, `real`, `money`, `smallmoney` → `numeric`
#' - `char`, `varchar`, `text`, `nchar`, `nvarchar`, `ntext` → `character`
#' - `date` → `Date`
#' - `datetime`, `datetime2`, `smalldatetime`, `datetimeoffset` → `POSIXct`
#' - `binary`, `varbinary` → `raw`
#'
#' Note: `bigint` is mapped to `numeric` rather than `integer`
#' because R integers can't handle the full range of TSQL `bigint` values.
#'
#' Column names in the returned data will match the casing used in `field_types`.
#' This ensures compatibility with functions like [tsql_chunk_loader()] that use
#' the same `field_types` specification. For example, if your data has columns
#' `c("NAME", "age")` and `field_types` specifies `c(Name = 'varchar(50)', Age = 'int')`,
#' the returned data will have columns `c("Name", "Age")`.
#'
#' @return
#' If `return_log = FALSE`, returns a data.table whose column
#' names match the casing specified in `field_types`. If `return_log = TRUE`,
#' returns a list with `data` (the converted data.table with names matching
#' `field_types` casing) and `conversion_log` (a data.table of results).
#'
#' @examples
#' \donttest{
#' # Example1: data with type mismatches
#'  library(data.table)
#'  mydt <- data.table(apgar_10 = c("8", "9", "7", "10"), #  should be int
#'                     birth_weight = c("3500", "2800", "4200", "3100"), # should be int
#'                     sex = c("M", "F", "M", "F")) # already character, matches char(1)
#'
#'  myfieldtypes <- c(apgar_10 = 'int',
#'                    birth_weight = 'int',
#'                    sex = 'char(1)')
#'
#'  # Basic conversion
#'  converted_data <- tsql_convert_types(ph.data = mydt,
#'                                       field_types = myfieldtypes)
#'
#'  # Conversion with detailed log
#'  result <- tsql_convert_types(ph.data = mydt,
#'                               field_types = myfieldtypes,
#'                               return_log = TRUE)
#'  converted_data <- result$data
#'  conversion_log <- result$conversion_log
#'
#' ###########################################################################
#' # Example2: showing column name casing behavior
#' mydt2 <- data.table(FIRST_name = c("Alice", "Bob"),
#'                     last_NAME = c("Smith", "Jones"),
#'                     AGE = c("25", "30"))
#'
#' # field_types uses mixed case
#' myfieldtypes2 <- c(First_Name = 'varchar(50)',
#'                    Last_Name = 'varchar(50)',
#'                    Age = 'int')
#'
#' # Returned data will have column names: First_Name, Last_Name, Age
#' converted_data2 <- tsql_convert_types(ph.data = mydt2,
#'                                       field_types = myfieldtypes2)
#' names(converted_data2) # Shows: "First_Name" "Last_Name" "Age"
#' }
#'
#' @export
#' @rdname tsql_convert_types
#' @import data.table
tsql_convert_types <- function(ph.data = NULL,
                               field_types = NULL,
                               validate_before = TRUE,
                               validate_after = TRUE,
                               verbose = TRUE,
                               return_log = FALSE) {

  # Visible bindings for data.table/check global variables ----
  original_type <- target_r_type <- target_tsql_type <- conversion_success <- notes <- NULL
  column <- needs_conversion <- NULL

  # Validate arguments ----
  if (is.null(ph.data)) {
    stop("\n\U1F6D1 You must specify a dataset (i.e., {ph.data} must be defined)")
  }
  if (!is.data.table(ph.data)) {
    if (is.data.frame(ph.data)) {
      setDT(ph.data)
    } else {
      stop("\n\U1F6D1 {ph.data} must be the name of a data.frame or data.table.")
    }
  }

  ph.data = copy(ph.data) # so original will not change due to set functions

  if (is.null(field_types) || !(is.character(field_types) && !is.null(names(field_types)) && all(nzchar(names(field_types))))) {
    stop('\n\U1F6D1 {field_types} must specify a named character vector of TSQL data types.')
  }

  if (length(field_types) == 0) {
    stop('\n\U1F6D1 {field_types} cannot be empty.')
  }

  if (!identical(sort(tolower(names(ph.data))), sort(tolower(names(field_types))))) {
    stop('\n\U1F6D1 Conversion requires exactly one TSQL field_type per column name in {ph.data}.')
  }

  if(!is.logical(validate_before)){
    stop('\n\U1F6D1 {validate_before} must be specified as a logical (i.e., TRUE, T, FALSE, or F)')
  }

  if(!is.logical(validate_after)){
    stop('\n\U1F6D1 {validate_after} must be specified as a logical (i.e., TRUE, T, FALSE, or F)')
  }

  if(!is.logical(verbose)){
    stop('\n\U1F6D1 {verbose} must be specified as a logical (i.e., TRUE, T, FALSE, or F)')
  }

  if(!is.logical(return_log)){
    stop('\n\U1F6D1 {return_log} must be specified as a logical (i.e., TRUE, T, FALSE, or F)')
  }

  # Match ph.data column name casing to that in field_types ----
  lower_to_field_type_map <- stats::setNames(names(field_types), tolower(names(field_types)))
  new_names <- lower_to_field_type_map[tolower(names(ph.data))] # reorder names from field_map to match column order in ph.data
  setnames(ph.data, new_names)

  # IF `validate_before = TRUE` ----
  if (validate_before) {
    if (verbose) {
      message('\U0001f50d Validating field types before conversion...')
    }

    tryCatch({
      suppressMessages(tsql_validate_field_types(ph.data, field_types)) # if there is no error, no need to convert anything
      if (verbose) {
        message('\U0001f642 Success! Your desired TSQL data types are already compatible with your dataset. No conversion needed.')
      }

      if (return_log) {
        return(list(
          data = ph.data,
          conversion_log = data.table(
            column = names(field_types),
            original_type = sapply(ph.data, function(x) class(x)[1]),
            target_r_type = "No conversion needed",
            target_tsql_type = field_types,
            conversion_success = TRUE,
            notes = "Already compatible"
          )
        ))
      } else {
        return(ph.data)
      }
    }, error = function(e) {
      if (verbose) {
        message('\u26A0\ufe0f Pre-conversion validation found issues. Continuing with conversion attempt(s)...')
      }
    })
  }

  # Define TSQL to R type mapping ----
  tsql_to_r_mapping <- list(
    tinyint = "integer",
    smallint = "integer",
    int = "integer",
    integer = "integer",
    bigint = "numeric", # R integers can't handle all BIGINT
    decimal = "numeric",
    numeric = "numeric",
    float = "numeric",
    real = "numeric",
    money = "numeric",
    smallmoney = "numeric",
    char = "character",
    varchar = "character",
    text = "character",
    nchar = "character",
    nvarchar = "character",
    ntext = "character",
    bit = "integer",
    date = "Date",
    datetime = "POSIXct",
    datetime2 = "POSIXct",
    smalldatetime = "POSIXct",
    datetimeoffset = "POSIXct",
    binary = "raw",
    varbinary = "raw"
  )

  # Helper function: map_tsql_to_r_class() ----
  map_tsql_to_r_class <- function(tsql_type) {
    base_type <- tolower(gsub("\\(.*\\)", "", trimws(tsql_type))) # drop specifiers in (), e.g., CHAR(12) > char
    if (!is.null(tsql_to_r_mapping[[base_type]])) {
      tsql_to_r_mapping[[base_type]]
    } else {"character"}  # default to character if can't be mapped
  }

  # Create shell conversion log ----
  conversion_log <- data.table(
    column = character(),
    original_type = character(),
    target_r_type = character(),
    target_tsql_type = character(),
    conversion_success = logical(),
    notes = character()
  )

  # Perform conversions ----
  # First, identify which columns actually need conversion
  all_column_info <- data.table(column = names(field_types))

  all_column_info[, target_r_class := vapply(field_types, map_tsql_to_r_class, character(1))]

  all_column_info[, orig_r_class := vapply(all_column_info$column,
                                           function(col) class(ph.data[[col]])[1],
                                           character(1))]

  all_column_info[, needs_conversion := orig_r_class != target_r_class]

  columns_needing_conversion <- all_column_info[(needs_conversion), column]

  if (verbose) {
    if (length(columns_needing_conversion) == 0) {
      message('\U0001f642 All columns already have compatible types. No conversions needed.')
    } else {
      message(paste0('\U0001f504 Attempting to convert ', length(columns_needing_conversion), ' of ', length(field_types), ' columns...'))
    }
  }

  # Perform actual conversions only on columns that need it
  for (col_name in columns_needing_conversion) {
    col_info <- all_column_info[column == col_name]
    tsql_type <- field_types[[col_name]]
    target_r_class <- col_info$target_r_class
    orig_r_class <- col_info$orig_r_class

    if (verbose) {
      message(col_name, ': ', orig_r_class, ' -> ', target_r_class)
    }

    # Attempt conversion using lossless_convert
    tryCatch({
      original_column <- ph.data[[col_name]]
      ph.data[, (col_name) := rads::lossless_convert(get(col_name), target_r_class, col_name)]

      # Check if conversion actually worked
      conversion_worked <- !identical(ph.data[[col_name]], original_column)

      if (conversion_worked) {
        success_note <- "Converted successfully"
      } else {
        success_note <- "Conversion not performed (would be lossy)"
      }

      # Log the result
      conversion_log <- rbindlist(list(conversion_log, data.table(
        column = col_name,
        original_type = orig_r_class,
        target_r_type = target_r_class,
        target_tsql_type = tsql_type,
        conversion_success = conversion_worked,
        notes = success_note
      )))

    }, error = function(e) {
      # Log errors
      conversion_log <<- rbindlist(list(conversion_log, data.table(
        column = col_name,
        original_type = orig_r_class,
        target_r_type = target_r_class,
        target_tsql_type = tsql_type,
        conversion_success = FALSE,
        notes = paste("Error:", e$message)
      )))

      if (verbose) {
        message(paste0('\U1F6D1 Error converting ', col_name, ': ', e$message))
      }
    })
  }

  # Add to log for columns that didn't need conversion
  columns_not_needing_conversion <- all_column_info[needs_conversion == FALSE]$column
  if (length(columns_not_needing_conversion) > 0) {
    for (col_name in columns_not_needing_conversion) {
      col_info <- all_column_info[column == col_name]
      conversion_log <- rbindlist(list(conversion_log, data.table(
        column = col_name,
        original_type = col_info$orig_r_class,
        target_r_type = col_info$target_r_class,
        target_tsql_type = field_types[[col_name]],
        conversion_success = TRUE,
        notes = "Already correct type"
      )))
    }
  }

  # Provide summary of conversions ----
  if (verbose) {
    successful <- conversion_log[conversion_success == TRUE, .N]
    failed <- conversion_log[conversion_success == FALSE, .N]

    message(paste0('\U0001f4ca Conversion summary: ', successful, ' successful, ', failed, ' failed'))

    if (failed > 0) {
      message('\U1F6D1 The following conversions failed:')
      failed_conversions <- conversion_log[conversion_success == FALSE]
      for (i in 1:nrow(failed_conversions)) {
        message(paste0('     ', failed_conversions$column[i], ': ', failed_conversions$notes[i]))
      }
    }
  }

  # IF `validate_after = TRUE` ----
  if (validate_after) {
    if (verbose) {
      message('\U0001f50d Validating field types after conversion...')
    }

    tryCatch({
      suppressMessages(tsql_validate_field_types(ph.data, field_types)) # if there are errors give more informative feedback
      if (verbose) {
        message('\U0001f642 Success! Post-conversion validation passed.')
      }
    }, error = function(e) {
      if (verbose) {
        message('\u26A0\ufe0f Post-conversion validation found remaining issues:')
        message(e$message)
        message('\U0001f4a1 Consider manually inspecting failed conversions or adjusting your data.')
      }
    })
  }

  # Return results ----
  if (return_log) {
    return(list(
      data = ph.data,
      conversion_log = conversion_log
    ))
  } else {
    return(ph.data)
  }
}

