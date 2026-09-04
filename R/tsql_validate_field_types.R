# tsql_validate_field_types() ----
#' Validates whether a named vector of TSQL data types is compatible with a
#' data.table
#'
#' @description
#' \code{tsql_validate_field_types} checks whether a named vector of TSQL data
#' types is compatible with a given data.table that you wish to upload to
#' Microsoft SQL Server. The function does not cover every possible situation!
#' For example, you might want to push your R '`POSIXct`' column to a SQL Server
#' table as an '`nvarchar()`' datatype, but this function will expect you to map
#' it to a more typical data type such as '`datetime`'. Think of this function
#' as a second set of eyes to make sure you didn't do something careless.
#'
#' @note Ported from the `rads` package. It has been migrated here in preparation for release of
#' `rads` version 2.0.0.
#'
#' @param ph.data The name of a single data.table/data.frame to be loaded to SQL
#'   Server.
#' @param field_types A named character vector with the desired TSQL datatypes
#'   for your upload. For example, `c(col1 = 'int', col2 = 'float', col3 =
#'   'date')`. Note that the names in `field_types` must be the same as the
#'   names in `ph.data`. This is often read into memory from a *.yaml file, but
#'   can also be manually created.
#'
#' @name tsql_validate_field_types
#'
#' @details Note that this function may not thoroughly evaluate if the allocated
#' length for character strings, i.e., `nvarchar()` and `varchar()`, is
#' sufficient.
#'
#' To use this function to
#' check the compatibility of field types in a yaml file with your dataset, do
#' the following:
#' 1) load the yaml file, e.g., `yaml <- yaml::read_yaml("X:/code/myyaml.yaml")`
#' 2) unlist the variable descriptions, e.g., `yaml_field_types = unlist(yaml$vars)`
#' 3) use `tsql_validate_field_types`, e.g., `tsql_validate_field_types(ph.data = mydt, field_types = yaml_field_types)`
#'
#' @examples
#' \donttest{
#' # example of a success
#'  library(data.table)
#'  mydt = data.table(col1 = 1:10000L,  # creates integers
#'                    col2 = 1:10000/3) # creates floats
#'  mydt[, col3 := as.Date(Sys.Date()) - col1] # creates dates
#'  mydt[, col4 := as.character(col3)] # create strings
#'  mydt[, col5 := Sys.time()] # create POSIXct
#'
#'  myfieldtypes <- c(col1 = 'int',
#'                    col2 = 'float',
#'                    col3 = 'date',
#'                    col4 = 'nvarchar(255)',
#'                    col5 = 'datetime')
#'
#'  tsql_validate_field_types(ph.data = mydt, field_types = myfieldtypes)
#'
#' }
#'
#' @export
#' @rdname tsql_validate_field_types
#' @import data.table

tsql_validate_field_types <- function(ph.data = NULL,
                                      field_types = NULL) {
  # Visible bindings for data.table/check global variables ----
      Rtypes <- RtypesDT <- TSQLtypesDT <- combotypesDT <- std_type <- NULL
      colname <- size <- value_range <- is_valid <- R_type <- tsql_type <- NULL
      is_compatible <- meets_constraints <- NULL

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

      ph.data = copy(ph.data) # copy ph.data so will not change the underlying file submitted to this function via `set` functions
      setnames(ph.data, tolower(names(ph.data))) # b/c TSQL normally case insensitive re: column names and this function is to validate data types compatability

      if (is.null(field_types) || !(is.character(field_types) && !is.null(names(field_types)) && all(nzchar(names(field_types))))) {
        stop('\n\U1F6D1 {field_types} must specify a named character vector of TSQL data types.')
      }

      if (!identical(sort(tolower(names(ph.data))), sort(tolower(names(field_types))))) {
        stop('\n\U1F6D1 Validation of TSQL data types necessitates exactly one TSQL datatype per column name in {ph.data}.')
      }

  # Define type compatibility and constraints ----
      type_compatibility <- list(
        integer = c("tinyint", "smallint", "int", "integer", "bigint", "bit", "float", "real"),
        numeric = c("tinyint", "smallint", "int", "integer", "bigint", "decimal", "numeric", "float", "real", "money", "smallmoney"),
        character = c("char", "varchar", "text", "nchar", "nvarchar", "ntext"),
        factor = c("char", "varchar", "text", "nchar", "nvarchar", "ntext"),
        logical = "bit",
        Date = "date",
        IDate = "date",
        POSIXct = c("datetime", "datetime2", "smalldatetime", "datetimeoffset"),
        raw = c("binary", "varbinary", "image"),
        integer64 = "bigint"
      )

      type_constraints <- list(
        tinyint = list(min = 0, max = 255),
        smallint = list(min = -32768, max = 32767),
        int = list(min = -2147483648, max = 2147483647),
        bigint = list(min = -9223372036854775808, max = 9223372036854775807)
      )

  # Define helper functions ----
    # Function to extract size from TSQL type ----
        extract_size <- function(type) {
          size <- gsub(".*\\((\\d+)\\).*", "\\1", type)
          if (size == type) NA_integer_ else as.integer(size)
        }

    # Function to check type compatibility ----
        check_compatibility <- function(R_type, tsql_type) {
          # Allow integer >> character types (varchar, char, nvarchar, nchar)
          if (R_type %in% c("integer", "integer64") && tsql_type %in% c("char", "varchar", "nchar", "nvarchar")) {
            return(TRUE)
          }

          compatible_types <- unlist(type_compatibility[R_type])
          tsql_type %in% compatible_types
        }

    # Function to check value constraints ----
        check_constraints <- function(column, tsql_type, size, R_type) {
          if (tsql_type %in% names(type_constraints)) { # check range of vals for variations of INT
            constraints <- type_constraints[[tsql_type]]
            non_na <- !is.na(column)
            all(column[non_na] >= constraints$min & column[non_na] <= constraints$max, na.rm = TRUE) &&
              # Add check for numeric to integer conversion
              if (R_type == "numeric") {
                all(column[non_na] == floor(column[non_na]), na.rm = TRUE)
              } else {
                TRUE # if R_type not numeric, must be integer -- defined in type compatibility
              }
          } else if (tsql_type == "bit" && R_type == "integer") {
            all(column %in% c(0, 1, NA), na.rm = TRUE)
          } else if (tsql_type %in% c("char", "varchar", "nchar", "nvarchar") && !is.na(size)) {
            all(nchar(as.character(column)) <= size, na.rm = TRUE)
          } else {
            TRUE
          }
        }

  # Generate R types data table ----
      RtypesDT <- data.table(
        colname = tolower(names(ph.data)),
        R_type = sapply(ph.data, \(x) class(x)[1]), # keep only first class if there is more than one, e.g., c("POSIXct", "POSIXt")
        key = "colname"
      )
      RtypesDT[R_type %in% c('POSIXt', 'POSIXlt'), R_type := 'POSIXct']
      RtypesDT[R_type == 'IDate', R_type := 'Date']

      valid_R_types <- unique(names(type_compatibility))
      if(nrow(RtypesDT[!R_type %in% valid_R_types]) > 0){
        stop(paste0("\n\U1F6D1\U0001f47f The following R classes (column data types) are not recognized: ",
                    paste0(unique(RtypesDT[!R_type %in% valid_R_types]$R_type), collapse = ','),
                    ".\n These data types are not currently supported for TSQL conversion.",
                    " If you think this is a mistake, please submit a GitHub issue."))
      }

  # Generate TSQL types data table ----
      TSQLtypesDT <- data.table(
        colname = tolower(names(field_types)),
        tsql_type = gsub("\\(.*$", "", tolower(field_types)), # drop off (###)
        size = sapply(field_types, extract_size),
        key = "colname"
      )

      valid_tsql_types <- unique(unlist(type_compatibility))
      if(nrow(TSQLtypesDT[!tsql_type %in% valid_tsql_types]) > 0){
        stop(paste0("\n\U1F6D1\U0001f47f The following TSQL field types are not recognized: ",
                    paste0(unique(TSQLtypesDT[!tsql_type %in% valid_tsql_types]$tsql_type), collapse = ','),
                    ".\n If you believe it is valid, please submit a GitHub issue."))
      }

  # Combine R and TSQL type information ----
      combotypesDT <- merge(RtypesDT, TSQLtypesDT, by = "colname")

  # Validate type compatibility and constraints ----
      combotypesDT[, is_compatible := mapply(check_compatibility,
                                             R_type,
                                             tsql_type)]

      combotypesDT[, meets_constraints := mapply(check_constraints,
                                                 ph.data[, .SD, .SDcols = colname],
                                                 tsql_type,
                                                 size,
                                                 R_type)]

      combotypesDT[is.na(meets_constraints), meets_constraints := TRUE]

  # Generate detailed validation results ----
      validation_results <- combotypesDT[, list(
        colname = colname,
        R_type = R_type,
        tsql_type = tsql_type,
        is_valid = is_compatible & meets_constraints,
        issue = fcase(
          R_type %in% c("integer", "integer64") & tsql_type %in% c("char","varchar","nchar","nvarchar"),
          "Warning: integer stored as character (allowed, but non-standard)",

          !is_compatible, "Incompatible types",

          !meets_constraints & R_type == "numeric" & tsql_type %in% c("tinyint", "smallint", "int", "bigint"),
          "Numeric values cannot be safely converted to integer",

          !meets_constraints, "Fails constraints",

          default = NA_character_
        )
      )]

  # Provide feedback to user ----
      # report if column is all NA
      na_cols <- ph.data[, lapply(.SD, function(x) all(is.na(x))), .SDcols = names(ph.data)]
      na_cols <- names(na_cols)[as.logical(na_cols)]
      if(length(na_cols) > 0){warning('\u26A0\ufe0f Validation may be flawed for the following variables because they are 100% missing: ', paste0(na_cols, collapse = ', '))}

      # Give warnings for nonstandard conversions
      warning_rows <- validation_results[grepl("^Warning:", issue)]

      if (nrow(warning_rows) > 0) {
        warning(
          "\nThe following columns have non-standard but allowed conversions:\n",
          paste0(
            "     column: ", warning_rows$colname,
            ", R Type: ", warning_rows$R_type,
            ", TSQL Type: ", warning_rows$tsql_type,
            ", issue: ", warning_rows$issue,
            collapse = "\n"
          )
        )
      }


      # report back overall validation
      if (all(validation_results$is_valid)) {
        message('\U0001f642 Success! Your desired TSQL data types are suitable for your dataset.')
      } else {
        invalid_columns <- validation_results[is_valid == FALSE]
        cat("\n================ INVALID FIELD TYPES ================\n")
        print(invalid_columns)
        cat("======================================================\n")
        stop(paste0('\n\U1F6D1\U0001f47f One or more columns did not align with the proposed TSQL field types.\n',
                    'See the printed table above for full details.'))
      }

  # Return validation results ----
    invisible(validation_results)
}


