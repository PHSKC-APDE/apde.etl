# etl_qa_run_pipeline() ... one function to run them all ----
#' @title Run ETL Quality Assurance Pipeline
#'
#' @description
#' This function runs a comprehensive quality assurance pipeline for ETL
#' (Extract, Transform, Load) processes.
#' It analyzes data for missingness, variable distributions, and optionally
#' checks compliance with CHI (Community Health Indicators) standards.
#'
#' @note
#' This function replaces the deprecated `etl_qa_run_pipeline` function from the
#' `apde` package.
#'
#' @param data_source_type Character string specifying the type of data source.
#' Must be one of `'r_dataframe'`, `'sql_server'`, or `'rads'`.
#' @param connection A DBIConnection object. *Required only when
#' `data_source_type = 'sql_server'`*
#' @param data_params List of data related parameters specific to the data source. Not all
#' parameters are needed for all data sources. Please review the examples for details.
#'
#' - `check_chi`: Logical vector of length 1. When `check_chi = TRUE`,
#'   function will add any available CHI related variables to `cols` and
#'   will assess whether their values align with standards in
#'   `rads.data::misc_chi_byvars`. Default is `FALSE`
#'
#' - `cols`: Character vector specifying the column names to analyze,
#'   e.g., `cols = c('race4', 'birth_weight_grams', 'birthplace_city')`
#'
#' - `time_range`: Character vector of length 2 specifying the start
#'   and end of the time range, e.g., `time_range = c(2015, 2024)`
#'
#' - `time_var`: Character string specifying the time interval variable,
#'   e.g., `time_var = 'chi_year'`
#'
#' - `data`: Name of a data.frame or data.table that you want to assess
#'   with this function, e.g., `data = myDataTable`. *Required only when
#'   `data_source_type = 'r_dataframe'`*.
#'
#' - `function_name`: Character string specifying the relevant
#'   `rads::get_data_xxx` function, e.g., `function_name = 'get_data_birth'`.
#'   *Required only when `data_source_type = 'rads'`*
#'
#' - `kingco`: Logical vector of length 1. Identifies whether you want
#'   limit the data to King County. *Required only when
#'   `data_source_type = 'rads'`*. Default is `kingco = TRUE`
#'
#' - `version`: Character string specifying either `'final'` or
#'   `'stage'`. *Required only when `data_source_type = 'rads'`*.
#'   Default is `version = 'stage'`
#'
#' - `schema_table`: The name of the schema and table to be accessed
#'   within the SQL Server `connection`. Must be in the form
#'   `myschema.mytable`, with a period as a separator.
#'   *Required only when `data_source_type = 'sql_server'`*
#'
#' @param output_directory Character string specifying the directory where output
#' files will be saved. If `NULL`, the current working directory is used.
#' Default is `output_directory = NULL`.
#' @param digits_mean Integer specifying the number of decimal places for rounding
#' the reported mean, median, min, and max. Default is `digits_mean = 3`.
#' @param digits_prop Integer specifying the number of decimal places for rounding
#' proportions. Default is `digits_prop = 3`.
#' @param abs_threshold Numeric threshold for flagging absolute percentage changes
#' in proportions. Permissible range is `[0, 100]`. Default is `abs_threshold = 3`.
#' @param rel_threshold Numeric threshold for flagging relative percentage changes
#' in means and medians. Permissible range is `[0, 100]`. Default is `rel_threshold = 2`.
#' @param distinct_threshold Minimum number of distinct values needed for
#' calculating the minimum, mean, median, and maximum values. If the number of
#' distinct values is under this threshold, it will be treated as a categorical. Default is
#' `distinct_threshold = 1`.
#'
#' @return A list containing the final results from the ETL QA pipeline. Specifically, it includes:
#' - `config`: Configuration settings used for the analysis
#' - `initial`: Initial ETL QA results
#' - `final`: Final ETL QA results - ready for reporting
#' - `exported`: File paths for exported tables and plots
#'
#' @details
#' The function provides identical output whether using `rads`, providing a
#' data.table that is in R's memory, or processing data directly on MS SQL Server.
#' The key is to correctly set up the arguments. Please refer to the examples
#' below for models that you should follow.
#'
#' @seealso
#' - [etl_qa_setup_config()] for Step 1: Creating the `config` object
#' - [etl_qa_initial_results()] for Step 2: Initial ETL QA analysis
#' - [etl_qa_final_results()] for Step 3: Final / formatted ETL QA analysis
#' - [etl_qa_export_results()] for Step 4: Export of tables and plots
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # The following three examples generate identical output:
#'
#' # Example with RADS
#' qa.rads <- etl_qa_run_pipeline(
#'   data_source_type = 'rads',
#'   data_params = list(
#'     function_name = 'get_data_birth',
#'     time_var = 'chi_year',
#'     time_range = c(2021, 2022),
#'     cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city',
#'              'num_prev_cesarean', 'mother_date_of_birth'),
#'     version = 'final',
#'     kingco = FALSE,
#'     check_chi = FALSE
#'   ),
#'   output_directory = 'C:/temp/'
#' )
#'
#'
#' # Example with R dataframe
#' birth_data <- rads::get_data_birth(year = c(2021:2022),
#'                                    kingco = F,
#'                                    cols = c('chi_age', 'race4',
#'                                    'birth_weight_grams', 'birthplace_city',
#'                                    'num_prev_cesarean', 'chi_year',
#'                                    'mother_date_of_birth'),
#'                                    version = 'final')
#'
#' qa.df <- etl_qa_run_pipeline(
#'   data_source_type = 'r_dataframe',
#'   data_params = list(
#'     data = birth_data,
#'     time_var = 'chi_year',
#'     time_range = c(2021, 2022),
#'     cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city',
#'              'num_prev_cesarean', 'mother_date_of_birth'),
#'     check_chi = FALSE
#'   ),
#'   output_directory = 'C:/temp/'
#' )
#'
#'
#' # Example with SQL Server
#' library(DBI)
#' myconnection <- rads::validate_hhsaw_key()
#' qa.sql <- etl_qa_run_pipeline(
#'   data_source_type = 'sql_server',
#'   connection = myconnection,
#'   data_params = list(
#'     schema_table = 'birth.final_analytic',
#'     time_var = 'chi_year',
#'     time_range = c(2021, 2022),
#'     cols =c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city',
#'             'num_prev_cesarean', 'mother_date_of_birth'),
#'     check_chi = FALSE
#'   ),
#'   output_directory = 'C:/temp/'
#' )
#'
#' # Confirmation that the results are identical
#' all.equal(qa.rads$final, qa.df$final)
#' all.equal(qa.rads$final, qa.sql$final)
#'
#' }
#'
#'
etl_qa_run_pipeline <- function(connection = NULL,
                                data_source_type,
                                data_params = list(),
                                output_directory = NULL,
                                digits_mean = 3,
                                digits_prop = 3,
                                abs_threshold = 3,
                                rel_threshold = 2,
                                distinct_threshold = 1) {
  # Capture the name of the data source if it's an R data.table/data.frame (annoying hack to extract name from within a list) ----
  if (data_source_type == 'r_dataframe' && 'data' %in% names(data_params)) {
    call <- match.call()  # Capture the original function call
    data_name <- deparse(call$data_params$data)  # Extract the data object name (note use of `call`)
    data_params$data_source <- data_name  # Assign it to the data source
  }

  # Validate arguments ----
  ## Validate data_source_type ----
  if (!data_source_type %in% c('r_dataframe', 'sql_server', 'rads')) {
    stop("\U0001f47f\ndata_source_type must be one of 'r_dataframe', 'sql_server', or 'rads'")
  }

  ## Validate connection ----
  if (data_source_type == 'sql_server' && !inherits(connection, "DBIConnection") && !is.null(connection)) {
    stop("\U0001f47f\nFor 'sql_server' data_source_type, connection must be a DBIConnection object")
  } else if (data_source_type == 'sql_server' && inherits(connection, "DBIConnection") && !is.null(connection) && !DBI::dbIsValid(connection)){
    stop("\U0001f47f\nYour `connection` argument is not a valid DBIConnection object. It may have been disconnected. Please reconnect and try again.")
  } else if (data_source_type == 'sql_server' && is.null(connection)) {
    stop("\U0001f47f\nFor 'sql_server' data_source_type, a DBIConnection object must be provided for the connection argument")
  } else if (data_source_type != 'sql_server' && !is.null(connection)) {
    warning("\U00026A0\nThe connection argument is ignored when data_source_type != 'sql_server'")
  }

  ## Validate data_params ----
  ### data_params is a list ----
  if (!is.list(data_params)) {
    stop("\U0001f47f\ndata_params must be a list")
  }

  ### data_params$check_chi ----
  if ('check_chi' %in% names(data_params)){
    if ((length(data_params$check_chi) == 1 && is.na(data_params$check_chi)) | !is.logical(data_params$check_chi)) {
      stop("\U0001f47f\nIf provided, `data_params$check_chi` must be a logical (TRUE | FALSE, or equivalently, T | F).")
    }
  } else {
    data_params$check_chi <- FALSE
  }

  ### data_params$cols ----
  if((!'cols' %in% names(data_params) || is.null(data_params$cols) ) & isFALSE(data_params$check_chi)){
    stop("\U0001f47f\nYou must specify the 'data_params$cols' argument when data_params$check_chi is FALSE or not provided.")
  } else if ((!'cols' %in% names(data_params) || is.null(data_params$cols) ) & isTRUE(data_params$check_chi)){
    warning("\U00026A0\nYou did not specify the 'data_params$cols' argument. Since data_params$check_chi = TRUE, the code will run, \n",
    "however it might encounter an error if there are no CHI related variables in your data.")
  }

  ### data_params$time_range ----
  if (!"time_range" %in% names(data_params) || !is.numeric(data_params$time_range) || length(data_params$time_range) != 2) {
    stop("\U0001f47f\n`data_params$time_range` must be provided and must be a numeric or integer vector of length 2.")
  } else if (data_params$time_range[1] > data_params$time_range[2]) {
    stop("\U0001f47f\nThe first value of `data_params$time_range` must be less than or equal to the second value.")
  }

  ### data_params$time_var ----
  if (!"time_var" %in% names(data_params)) {
    stop("\U0001f47f\n`data_params$time_var` is missing and must be provided. It should be the name of a single time variable, e.g., 'chi_year' or 'survey_year'")
  } else if (!is.character(data_params$time_var) || length(data_params$time_var) != 1) {
    stop("\U0001f47f\n`data_params$time_var` must be the name of a single time variable, e.g., 'chi_year' or 'survey_year'.")
    }

  ### data_params$data ----
  if (data_source_type == 'r_dataframe') {
    if (!'data' %in% names(data_params) || !is.data.frame(data_params$data)) {
      stop("\U0001f47f\nFor 'r_dataframe' type, data_params must include a 'data' element that is a data.frame or data.table")
    }
  }

  ### data_params$function_name ----
  if (data_source_type == 'rads' && !'function_name' %in% names(data_params)) {
    stop("\U0001f47f\nFor 'rads' data_source_type, data_params must include a 'function_name'. Options are limited to:\n",
         sub(', ([^,]*)$', ' & \\1', paste0(suppressWarnings(grep('get_data_', ls(getNamespace("rads")), value = T)), collapse = ', ')), ".")
  } else if (data_source_type == 'rads' && !data_params$function_name %in% suppressWarnings(grep('get_data_', ls(getNamespace("rads")), value = T))){
    stop("\U0001f47f\nThe data_params$function_name you provided is invalid. Available options are limited to:\n",
         sub(', ([^,]*)$', ' & \\1', paste0(suppressWarnings(grep('get_data_', ls(getNamespace("rads")), value = T)), collapse = ', ')), ".")
  }

  ### data_params$kingco ----
    if (data_source_type == 'rads') {
      if(!'kingco'%in% names(data_params)){
        message("data_params$kingco not provided. Defaulting to TRUE.")
        data_params$kingco <- TRUE
      } else if (!is.logical(data_params$kingco) || length(data_params$kingco) != 1 || is.na(data_params$kingco)) {
        stop("\U0001f47f\nIf provided, `data_params$kingco` must be a single logical value (TRUE or FALSE).")
      }
    }

  ### data_params$version ----
    if (data_source_type == 'rads') {
      if (!'version' %in% names(data_params)) {
        message("data_params$version not provided. Defaulting to 'stage'.")
        data_params$version <- 'stage'
      } else if (!(data_params$version %in% c('stage', 'final'))) {
        stop("\U0001f47f\nInvalid value for data_params$version. It must be either 'stage' or 'final'.")
      }
    }

  ### data_params$schema_table ----
  if (data_source_type == 'sql_server' && !'schema_table' %in% names(data_params)) {
    stop("\U0001f47f\nFor 'sql_server' data_source_type, data_params$schema_table must be provided in the format 'schema.table'")
  }

  ## Validate output directory ----
  if(is.null(output_directory)){
    warning(paste0("The output_directory argument was not specified, so output will be saved in your present working directory:\n",
                   getwd()))
  }
  if(!is.null(output_directory) && !dir.exists(output_directory)){
    stop("\U0001f47f\nYou have specified an output_directory that does not exist. Please specify an existing directory.")
  }

  ## Validate numeric parameters ----
  if (!is.numeric(digits_mean) || digits_mean < 0 || (digits_mean %% 1) != 0) {
    stop("\U0001f47f\ndigits_mean must be a non-negative integer")
  }
  if (!is.numeric(digits_prop) || digits_prop < 0 || (digits_prop %% 1) != 0) {
    stop("\U0001f47f\ndigits_prop must be a non-negative integer")
  }
  if (!is.numeric(abs_threshold) || abs_threshold < 0 || abs_threshold > 100) {
    stop("\U0001f47f\nabs_threshold must be a non-negative number [0, 100]")
  }
  if (!is.numeric(rel_threshold) || rel_threshold < 0 || rel_threshold > 100) {
    stop("\U0001f47f\nrel_threshold must be a non-negative number [0, 100]")
  }

  # Step 1: Configure data source ----
  message('Creating config object ... running etl_qa_setup_config()\n')
  config <- etl_qa_setup_config(
    data_source_type = data_source_type,
    connection = connection,
    data_params = data_params,
    output_directory = output_directory,
    digits_mean = digits_mean,
    digits_prop = digits_prop,
    abs_threshold = abs_threshold,
    rel_threshold = rel_threshold,
    distinct_threshold  = distinct_threshold
  )

  # Step 2: Initial QA results ----
  message('Analyzing data ... running etl_qa_initial_results()\n')
  qa_initial <- etl_qa_initial_results(config)

  # Step 3: Final QA results ----
  message('Preparing results ... running etl_qa_final_results()\n')
  qa_final <- etl_qa_final_results(qa_initial, config)

  # Step 4: Visualize QA data ----
  message('Visualizing data ... running etl_qa_export_results()\n')
  exported <- etl_qa_export_results(qa_final, config)

  # Return results ----
  return(list(
    config = config,
    initial = qa_initial,
    final = qa_final,
    exported = exported
  ))
}

# Tiny helper function ----
#' @title Helper function to provide a default value for NULL
#'
#' @description
#' This infix function returns the first argument if it's not NULL,
#' otherwise it returns the second argument.
#'
#' @name default_value
#' @rdname default_value
#' @param x The primary value to check
#' @param y The default value to use if x is NULL
#'
#' @return The value of x if it's not NULL, otherwise the value of y
#'
#' @examples
#' # Basic usage
#' NULL %||% 5  # Returns 5
#' 10 %||% 5    # Returns 10
#'
#' # In a function
#' f <- function(x = NULL) {
#'   x %||% "default"
#' }
#' f()        # Returns "default"
#' f("value") # Returns "value"
#'
#' @keywords internal
#' @noRd
#'
`%||%` <- function(x, y) if (is.null(x)) y else x

#--------------------------------- ----
#---- STEP 1: Create config object ----
#--------------------------------- ----
# etl_qa_setup_config() ----
#' @title Set up configuration for ETL QA pipeline
#'
#' @description
#' This function creates a configuration object for the ETL QA pipeline based on
#' the provided parameters. It is the first step called upon by
#' [etl_qa_run_pipeline()].
#'
#' @param data_source_type Character string specifying the type of data source
#' @param connection A DBIConnection object for SQL Server connections
#' @param data_params List of parameters specific to the data source
#' @param output_directory Character string specifying the output directory
#' @param digits_mean Integer specifying decimal places for mean rounding
#' @param digits_prop Integer specifying decimal places for proportion rounding
#' @param abs_threshold Numeric threshold for flagging absolute changes
#' @param rel_threshold Numeric threshold for flagging relative changes
#' @param distinct_threshold Minimum number of distinct values needed for
#' calculating the minimum, median, and maximum values.
#'
#' @details
#' This is an *internal function* accessible only by use of `:::`, for example,
#' `apde.etl:::etl_qa_setup_config(...)`. The arguments are identical to those used
#' by [etl_qa_run_pipeline()]. Please review that helpful for details.
#'
#' @return An S3 object of class "qa_data_config", which is a list containing the configuration settings.
#'
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' # The following examples generate config objects which can be passed to
#' # etl_qa_initial_results()
#'
#' # Example with RADS
#' config.rads <- etl_qa_setup_config(
#'   data_source_type = 'rads',
#'   data_params = list(
#'     function_name = 'get_data_birth',
#'     time_var = 'chi_year',
#'     time_range = c(2021, 2022),
#'     cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city',
#'              'num_prev_cesarean', 'mother_date_of_birth'),
#'     version = 'final',
#'     kingco = FALSE,
#'     check_chi = FALSE
#'   ),
#'   output_directory = 'C:/temp/'
#' )
#' class(config.rads)
#'
#'
#' # Example with R data.frame
#' birth_data <- rads::get_data_birth(year = c(2021:2022),
#'                              kingco = F,
#'                              cols = c('chi_age', 'race4', 'birth_weight_grams',
#'                              'birthplace_city', 'num_prev_cesarean',
#'                              'chi_year', 'mother_date_of_birth'),
#' )
#' config.df <- etl_qa_setup_config(
#'   data_source_type = 'r_dataframe',
#'   data_params = list(
#'     data = birth_data,
#'     time_var = 'chi_year',
#'     time_range = c(2021, 2022),
#'     cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city',
#'              'num_prev_cesarean', 'mother_date_of_birth'),
#'     check_chi = FALSE
#'   ),
#'   output_directory = 'C:/temp/'
#' )
#' class(config.df)
#'
#'
#' # Example with SQL Server
#' library(DBI)
#' myconnection <- rads::validate_hhsaw_key()
#' config.sql <- etl_qa_setup_config(
#'   data_source_type = 'sql_server',
#'   connection = myconnection,
#'   data_params = list(
#'     schema_table = 'birth.final_analytic',
#'     time_var = 'chi_year',
#'     time_range = c(2021, 2022),
#'     cols =c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city',
#'             'num_prev_cesarean', 'mother_date_of_birth'),
#'     check_chi = FALSE
#'   ),
#'   output_directory = 'C:/temp/'
#' )
#' class(config.sql)
#'
#' }
#'
#'
etl_qa_setup_config <- function(data_source_type,
                                connection = NULL,
                                data_params = list(),
                                output_directory = NULL,
                                digits_mean = 0,
                                digits_prop = 3,
                                abs_threshold = 3,
                                rel_threshold = 2,
                                distinct_threshold = 1) {
  # Capture the name of the data.table/data.frame and add to params (need to do first, before any arguments evaluated or modified)
  if(is.null(data_params$data_source) & 'data' %in% names(data_params)){ # will already exist if called upon by etl_qa_run_pipeline()
    call <- match.call()  # Capture the original function call
    data_name <- deparse(call$data_params$data)  # Extract the data object name (note use of `call`)
    data_params$data_source <- data_name  # Assign it to the data source
  }

  # Validate input parameters
  if (!data_source_type %in% c('r_dataframe', 'sql_server', 'rads')) {
    stop("\U0001f47f\ndata_source_type must be 'r_dataframe', 'sql_server', or 'rads'")
  }

  # Validate output_directory
  if (!is.null(output_directory)) {
    if (!dir.exists(output_directory)) {
      stop("\U0001f47f\nYou have specified an output_directory that does not exist. Please specify an existing directory.")
    }
  } else {
    output_directory <- getwd()
    warning("\U00026A0\nNo output_directory specified. Using current working directory: ", output_directory, ".")
  }

  # Validate check_chi
  if ('check_chi' %in% names(data_params)){
    if ((length(data_params$check_chi) == 1 && is.na(data_params$check_chi)) | !is.logical(data_params$check_chi)) {
      stop("\U0001f47f\nIf provided, `data_params$check_chi` must be a logical (TRUE | FALSE, or equivalently, T | F).")
    }
  } else {
    data_params$check_chi <- FALSE
  }

  # Create the base configuration object
  config <- list(
    data_source_type = data_source_type,
    connection = connection,
    data_params = data_params,
    output_directory = output_directory,
    digits_mean = digits_mean,
    digits_prop = digits_prop,
    abs_threshold = abs_threshold,
    rel_threshold = rel_threshold,
    distinct_threshold = distinct_threshold
  )

  # Add specific configurations based on data_source_type
  if (data_source_type == 'r_dataframe') {
    if (!'data' %in% names(data_params) || !is.data.frame(data_params$data)) {
      stop("\U0001f47f\nFor 'r_dataframe' type, data_params must include a 'data' element that is a data.frame or data.table")
    }

    config$process_location <- 'r'

  } else if (data_source_type == 'sql_server') {
    if (!inherits(connection, "DBIConnection")) {
      stop("\U0001f47f\nFor 'sql_server' data_source_type, connection must be a DBIConnection object")
    }
    if (!'schema_table' %in% names(data_params)) {
      stop("\U0001f47f\nFor 'sql_server' data_source_type, data_params must include a 'schema_table' in the format 'schema.table'")
    }
    if (!grepl("\\.", data_params$schema_table)) {
      stop("\U0001f47f\n'schema_table' should be in the format 'schemaName.tableName', with a period between the schema and table.")
    }
    config$process_location <- 'sql'
  } else if (data_source_type == 'rads') {
    if (!'function_name' %in% names(data_params)) {
      stop("\U0001f47f\nFor 'rads' type, data_params must include a 'function_name'")
    }
    if (!grepl('^get_data_', data_params$function_name)){
      stop("\U0001f47f\nFor 'rads' data_source_type, data_params must include a 'function_name' that begins with 'get_data_', e.g., 'get_data_birth'")
    }
    # Add 'kingco' and 'version' only for 'rads' type
    config$data_params$kingco <- data_params$kingco %||% TRUE
    config$data_params$version <- data_params$version %||% 'stage'

    config$process_location <- 'r'
  }

  # Add time interval configuration
  if (!'time_var' %in% names(data_params)) {
    stop("\U0001f47f\ndata_params must include a 'time_var' specifying the time interval variable")
  }
  config$time_var <- data_params$time_var

  # Add time range if provided
  if ('time_range' %in% names(data_params)) {
    if (!is.vector(data_params$time_range) || length(data_params$time_range) != 2) {
      stop("\U0001f47f\n'time_range' should be a vector of length 2 specifying the start and end of the time range")
    }
    config$time_range <- data_params$time_range
  }

  # Add common parameters
  config$data_params$cols <- data_params$cols %||% NULL

  # Create and return an S3 object
  structure(config, class = "qa_data_config")
}
#------------------------- ----
#---- STEP 2: Analyze data ----
#------------------------- ----
# etl_qa_initial_results() ----
#' @title Initial QA results for ETL QA pipeline
#'
#' @description
#' This function performs the core analysis for the ETL QA pipeline, processing
#' data based on the provided configuration. It is the second step run by
#' [etl_qa_run_pipeline()].
#'
#' @details
#' This is an *internal function* accessible only by use of `:::`, for example,
#' `apde.etl:::etl_qa_initial_results(...)`.
#'
#' @param config An S3 object of class "qa_data_config" containing configuration settings.
#'
#' @return A list of raw analytic output. The table structure may differ slightly depending on the original data_source. The list items include:
#' - `missing_data`: The proportion of missing data for each variable and time point
#' - `vals_continuous`: The minimum, median, mean, and maximum for all numeric variables with >= your specified `distinct_threshold` unique values
#' - `vals_date`: The minimum, median, and maximum for all date / datetime variables with >= your specified `distinct_threshold` unique values
#' - `vals_categorical`: A frequency table of the top 8 most frequent values
#'         of categorical variable (and numerics or dates with < your specified `distinct_threshold` distinct values) PLUS a rows for `NA`
#'         PLUS a row for all 'Other values'
#' - `chi_standards`: Comparison of CHI (Community Health Indicator) variables values with those expected based on `rads.data::misc_chi_byvars`
#'
#' @examples
#' \dontrun{
#'
#' # Step 1: generate a config object
#' myconfig <- etl_qa_setup_config(
#'   data_source_type = 'rads',
#'   data_params = list(
#'     function_name = 'get_data_birth',
#'     time_var = 'chi_year',
#'     time_range = c(2021, 2022),
#'     cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city',
#'              'num_prev_cesarean', 'mother_date_of_birth'),
#'     version = 'final',
#'     kingco = FALSE,
#'     check_chi = FALSE
#'   ),
#'   output_directory = 'C:/temp/'
#' )
#'
#'
#' # Step 2: perform the calculations
#' initial_results <- etl_qa_initial_results(myconfig)
#'
#' # Peek at the tables
#' head(initial_results$missing_data)
#' head(initial_results$vals_categorical)
#' head(initial_results$vals_continuous)
#' head(initial_results$vals_date)
#'
#' }
#'
#' @keywords internal
#'
#'
etl_qa_initial_results <- function(config) {
  # Get the list of data.tables based on the data source type
  data_list <- NULL
  if (config$data_source_type == 'r_dataframe') {
    data_list <- process_r_dataframe(config)
  } else if (config$data_source_type == 'rads') {
    data_list <- process_rads_data(config)
  } else if (config$data_source_type == 'sql_server') {
    data_list <- process_sql_server(config)
  } else {
    stop("\U0001f47f\nInvalid data source type")
  }

  # Order the rows in the data.tables in the list
  data.table::setorderv(data_list$missing_data, c('varname', config$time_var), c(1, 1))
  if('vals_continuous' %in% names(data_list) && nrow(data_list$vals_continuous) > 0){data.table::setorderv(data_list$vals_continuous, c('varname', config$time_var), c(1, 1))}
  if('vals_date' %in% names(data_list) && nrow(data_list$vals_date) > 0){data.table::setorderv(data_list$vals_date, c('varname', config$time_var), c(1, 1))}
  if('vals_categorical' %in% names(data_list) && nrow(data_list$vals_categorical) > 0){data.table::setorderv(data_list$vals_categorical, c('varname', config$time_var, 'proportion'), c(1, 1, -1))}

  # Return the modified list
  return(data_list)
}

# process_r_dataframe() - Function to process data.tables / data.frames ----
#' Process R dataframe for ETL QA
#'
#' Used by `etl_qa_initial_results()`
#'
#' @keywords internal
#' @noRd
#'
#'
process_r_dataframe <- function(config) {
  dt <- data.table::setDT(config$data_params$data)

  # Filter by time range if specified ----
  if (!is.null(config$time_range)) {
    dt <- dt[get(config$time_var) >= config$time_range[1] &
               get(config$time_var) <= config$time_range[2]]
  }

  # Identify CHI variables (if needed and only for data.frame since rads data filtered already) ----
  if (config$data_source_type == 'r_dataframe') {
    possiblecols <- names(dt)

    if(!config$data_params$time_var %in% possiblecols){
      stop("\U1F6D1\nThe variable specified in data_params$time_var is not available in this dataset.")
    }

    if(isTRUE(config$data_params$check_chi)){
      byvars <- unique(rads.data::misc_chi_byvars$varname)
      chivars <- c(grep('^chi_', possiblecols, value = TRUE))
    } else {byvars <- NULL; chivars <- NULL}

    config$data_params$cols <- sort(intersect(
      unique(c(chivars,
               byvars,
               config$data_params$cols,
               ifelse(isTRUE(config$data_params$kingco), 'chi_geo_kc', '')
      )),
      possiblecols))
  }

  # Ensure there is at least one varname in cols ----
  if(!'cols' %in% names(config$data_params) || length(config$data_params$cols) == 0 || all(config$data_params$time_var == config$data_params$cols)){
    stop("\U1F6D1\nNo valid 'cols' have been selected. Please enter valid column names in the data_params@cols argument.")
  }

  # Select specified columns ----
  if (!is.null(config$data_params$cols)) {
    dt <- dt[, unique(c(config$time_var, config$data_params$cols)), with = FALSE]
  }

  # Calculate missing data ----
  missing_data <- suppressWarnings(data.table::melt(dt, id.vars = c(config$time_var), measure.vars = setdiff(names(dt), c(config$time_var)))) # melt dt wide to long format

  missing_data <- missing_data[, list(nrow = sum(is.na(value)),
                                   proportion = sum(is.na(value)) / .N),
                               by = list(get(config$time_var), variable)]

  data.table::setnames(missing_data, c("get", "variable"), c(config$time_var, "varname"))
  missing_data[, varname := as.character(varname)]

  # Calculate summary statistics for continuous variables ----
  numeric_cols <- setdiff(names(dt)[sapply(dt, function(x) is.numeric(x) && length(unique(x)) >= config$distinct_threshold)], config$time_var) # identify numerics with >= distinct_threshold unique values

  if(length(numeric_cols) > 0){
    dt[, (numeric_cols) := lapply(.SD, as.double), .SDcols = numeric_cols]
    vals_continuous <- data.table::melt(dt, id.vars = c(config$time_var), measure.vars = numeric_cols, variable.name = 'varname') # melt dt wide to long format

    vals_continuous <- vals_continuous[, list(mean = as.double(mean(value, na.rm = TRUE)), # calculate stats
                                           median = as.double(stats::median(value, na.rm = TRUE)),
                                           min = as.double(min(value, na.rm = TRUE)),
                                           max = as.double(max(value, na.rm = TRUE))), by = list(get(config$time_var), varname)]

    data.table::setnames(vals_continuous,  c("get"), c(config$time_var))
    vals_continuous[, varname := as.character(varname)]} else {
      vals_continuous = data.table::data.table() # create a data.table with zero columns and zero rows, just to have a data.table object so rest of code will work
    }

  # Calculate summary statistics for datetime ----
  date_cols <- setdiff(names(dt)[sapply(dt, function(x) (inherits(x, "Date") || inherits(x, "POSIXt") || inherits(x, "POSIXct")) && length(unique(x)) >= config$distinct_threshold)], config$time_var)

  if(length(date_cols) > 0){
    dt[, (date_cols) := lapply(.SD, as.Date), .SDcols = date_cols] # convert datetime to actual date

    vals_date <- data.table::melt(dt, id.vars = c(config$time_var), measure.vars = date_cols, variable.name = 'varname') # melt dt wide to long format

    vals_date <- vals_date[, list(median = stats::median(value, na.rm = TRUE),
                               min = min(value, na.rm = TRUE),
                               max = max(value, na.rm = TRUE)), by = list(get(config$time_var), varname)]

    data.table::setnames(vals_date, c("get"), c(config$time_var))
    vals_date[, varname := as.character(varname)]
  } else {
    vals_date = data.table::data.table() # create a data.table with zero columns and zero rows, just to have a data.table object so rest of code will work
  }

  # Calculate categorical value frequencies ----
  categorical_cols <- setdiff(names(dt), c(config$time_var, numeric_cols, date_cols)) # categorical and numeric with <= distinct_threshol unique values

  if(length(categorical_cols) > 0){
    dt[, (categorical_cols) := lapply(.SD, as.character), .SDcols = categorical_cols]
    vals_categorical <- data.table::melt(dt, id.vars = config$time_var, measure.vars = categorical_cols, # wide to long
                             value.name = "value", variable.name = 'varname', variable.factor = FALSE)

    vals_categorical <- vals_categorical[, list(count = .N), by = c(config$time_var, 'varname', 'value')]
  } else {
    vals_categorical = data.table::data.table() # create a data.table with zero columns and zero rows, just to have a data.table object so rest of code will work
  }

  # Keep only top 8 most common / frequent categorical values (PLUS NA) ----
  vals_categorical <- keep_top_8(categorical_freq = vals_categorical, config = config)

  # Comparison with CHI standards (if needed) ----
  if(isTRUE(config$data_params$check_chi)){
    # Get all gold standard CHI varnames and groups
    chi_std <- unique(rads.data::misc_chi_byvars[, list(varname, group, chi = 1L)])

    # Identify all categorical chi variables that are in the data.frame/data.table
    categorical_cols <- setdiff(names(dt), c(config$time_var, numeric_cols, date_cols))
    chi_dtvars <- unique(intersect(categorical_cols, unique(chi_std$varname)))

    # Limit CHI gold standard table to just variables that are in the data.frame/data.table
    chi_std <- chi_std[varname %in% chi_dtvars]

    # Generate & tidy table of varnames and values by year ----
    your_data <- data.table::rbindlist(lapply(chi_dtvars, function(col) {
      unique(dt[, list(varname = col,
                    group = as.character(.SD[[col]])),
                by = chi_year,
                .SDcols = col
      ][, your_data := 1L])
    }))

    your_data <- your_data[!is.na(group)]

    # Merge CHI standards onto actual data ----
    chi_std_comparison <- merge(your_data,
                                chi_std,
                                by = c('varname', 'group'),
                                all = T)[, list(chi_year, varname, group, your_data, chi)]

    chi_std_comparison[is.na(your_data), your_data := 0]
    chi_std_comparison[is.na(chi), chi := 0]

    # use summary function
    comp_2_chi_std(chi_std_comparison)
  } else {
    chi_std_comparison = data.table::data.table() # create a data.table with zero columns and zero rows, just to have a data.table object so rest of code will work
  }

  # Create list for return / export ----
  list(missing_data = missing_data,
       vals_categorical = vals_categorical,
       vals_continuous = vals_continuous,
       vals_date = vals_date,
       chi_standards = chi_std_comparison)
}

# process_rads_data() - Function to process rads::get_data_xxx() ----
#' Process RADS data for ETL QA
#'
#' Used by `etl_qa_initial_results()`
#'
#' @keywords internal
#' @noRd
#'
#'
process_rads_data <- function(config) {
  # Validate config$data_params$kingco
  config$data_params$kingco <- config$data_params$kingco %||% TRUE
  if (!is.logical(config$data_params$kingco) || length(config$data_params$kingco) != 1 || is.na(config$data_params$kingco)) {
    stop("\U0001f47f\nIf provided, `config$data_params$kingco` must be a single logical value (TRUE or FALSE).")
  }

  # Validate config$data_params$version
  config$data_params$version <- config$data_params$version %||% 'stage'
  if (!(config$data_params$version %in% c('stage', 'final'))) {
    stop("\U0001f47f\nInvalid value for config$data_params$version. It must be either 'stage' or 'final'.")
  }

  # Identify CHI variables (if needed) ----
  possiblecols <- rads::quiet(rads::list_dataset_columns(gsub('get_data_', '', config$data_params$function_name))[]$var.names)

  if(!config$data_params$time_var %in% possiblecols){
    stop("\U1F6D1\nThe variable specified in data_params$time_var is not available in this dataset.")
  }

  if(isTRUE(config$data_params$check_chi)){
    byvars <- unique(rads.data::misc_chi_byvars$varname)
    chivars <- c(grep('^chi_', possiblecols, value = TRUE))
  } else {byvars <- NULL; chivars <- NULL}

  config$data_params$cols <- sort(intersect(
    unique(c(chivars,
             byvars,
             config$data_params$cols,
             'chi_year',
             ifelse(isTRUE(config$data_params$kingco), 'chi_geo_kc', '')
    )),
    possiblecols))

  # Use the appropriate get_data_xxx function ----
  data_func <- utils::getFromNamespace(config$data_params$function_name, "rads") # dynamically get the get_data_xxx function without loading all of rads

  dt <- data_func(year = config$time_range[1]:config$time_range[2],
                  cols = unique(c(config$time_var, config$data_params$cols)),
                  version = config$data_params$version,
                  kingco = config$data_params$kingco)
  config$data_params$data <- dt

  # Now process the data similarly to process_r_dataframe ----
  process_r_dataframe(config)
}

# process_sql_server() - Function to process data on SQL Server ----
#' Process SQL Server data for ETL QA
#'
#' used by `etl_qa_initial_results()`
#'
#' @keywords internal
#' @noRd
#'
#'
process_sql_server <- function(config) {
  # Confirm table exists ----
  myTableID = DBI::Id(schema = strsplit(config$data_params$schema_table, "\\.")[[1]][1],
                      table = strsplit(config$data_params$schema_table, "\\.")[[1]][2])

  if(!DBI::dbExistsTable(conn = config$connection, myTableID)){
    stop("\n\U1F6D1\nThe table specified by the data_params$schema_table argument does not exist.")
  }

  # Identify CHI variable (if needed) ----
  possiblecols <- names(DBI::dbGetQuery(conn = config$connection, glue::glue("SELECT TOP(0) * FROM {config$data_params$schema_table}")))

  if(!config$data_params$time_var %in% possiblecols){
    stop("\n\U1F6D1\nThe variable specified in data_params$time_var is not available in this dataset.")
  }

  if(isTRUE(config$data_params$check_chi)){
    byvars <- unique(rads.data::misc_chi_byvars$varname)
    chivars <- c(grep('^chi_', possiblecols, value = TRUE))
  } else {byvars <- NULL; chivars <- NULL}

  config$data_params$cols <- sort(intersect(
    unique(c(chivars,
             byvars,
             config$data_params$cols,
             ifelse(isTRUE(config$data_params$kingco), 'chi_geo_kc', '')
    )),
    possiblecols))

  # Ensure there is at least one varname in cols ----
  if(!'cols' %in% names(config$data_params) || length(config$data_params$cols) == 0){
    stop("\U1F6D1\nNo valid 'cols' have been selected. Please enter valid column (variable) names in the data_params argument.")
  }

  # Identify config col types ----
  config <- split_column_types(config)

  # Generate missing table ----
  missing_query <- generate_missing_query(config)
  missing_data <- data.table::setDT(DBI::dbGetQuery(config$connection, missing_query))

  # Generate numeric table ----
  if(length(config$data_params$cols.numeric) != 0){
    numeric_query <- generate_numeric_query(config)
    vals_continuous <- data.table::setDT(DBI::dbGetQuery(config$connection, numeric_query))
  } else  {vals_continuous <- data.table::data.table()} # empty data.table so rest of code will work

  # Generate date table ----
  if(length(config$data_params$cols.datetime) != 0){
    date_query <- generate_date_query(config)
    vals_date <- data.table::setDT(DBI::dbGetQuery(config$connection, date_query))
  } else { vals_date <- data.table::data.table()} # empty data.table so rest of code will work

  # Generate categorical table ----
  # add numeric/date vars with < 6 values to vector of character vars
  if(exists('vals_continuous')){
    config$data_params$cols.character <- c(config$data_params$cols.character,
                                           setdiff(config$data_params$cols.numeric, unique(vals_continuous$varname)))
  }
  if(exists('vals_date')){
    config$data_params$cols.character <- c(config$data_params$cols.character,
                                           setdiff(config$data_params$cols.datetime, unique(vals_date$varname)))
  }

  if(length(config$data_params$cols.character) != 0){
    categorical_query <- generate_categorical_query(config)
    categorical_freq <- data.table::setDT(DBI::dbGetQuery(config$connection, categorical_query))
    categorical_freq[value == 'NULL', value := NA] # Change 'NULL' to true NA
  } else {categorical_freq <- data.table::data.table() } # empty data.table so rest of code will work

  # Keep only top 8 most common / frequent categorical values (PLUS NA) ----
    vals_categorical <- keep_top_8(categorical_freq = categorical_freq, config = config)

  # Comparison with CHI standards (if needed) ----
  if(isTRUE(config$data_params$check_chi)){
    # Get all gold standard CHI varnames and groups
    # use unique(...) because some varname group combos duplicated because birth
    # data has different `cat` than other data
    chi_std <- unique(rads.data::misc_chi_byvars[, list(varname, group, chi = 1L)])

    # Limit chi_std to categorical variables in frequency table
    chi_std <- chi_std[varname %in% unique(categorical_freq$varname)]

    # Limit frequency table to CHI varnames
    categorical_freq <- categorical_freq[varname %in% unique(chi_std$varname)]

    # Tidy frequency table
    categorical_freq <- categorical_freq[, list(chi_year, varname, group = value, your_data = 1L)]
    categorical_freq <- categorical_freq[!is.na(group)]

    # Merge CHI standards onto actual data ----
    chi_std_comparison <- merge(categorical_freq,
                                chi_std,
                                by = c('varname', 'group'),
                                all = T)[, list(chi_year, varname, group, your_data, chi)]

    chi_std_comparison[is.na(your_data), your_data := 0]
    chi_std_comparison[is.na(chi), chi := 0]

    # use summary function
    comp_2_chi_std(chi_std_comparison)
  } else {
    chi_std_comparison = data.table::data.table() # create a data.table with zero columns and zero rows, just to have a data.table object so rest of code will work
  }

  # Create list for export ----
  list(missing_data = missing_data,
       vals_categorical = vals_categorical,
       vals_continuous = vals_continuous,
       vals_date = vals_date,
       chi_standards = chi_std_comparison)
}

# Helper functions in R code ----
## comp_2_chi_std() ----
#' Compare data to CHI standards in rads.data::misc_chi_byvars
#'
#' `used by process_sql_server()` & 'process_r_dataframe()'
#'
#' @keywords internal
#' @noRd
#'
#'
comp_2_chi_std <- function(myCHIcomparison){
  # Expects data.table with chi_year <integer>, varname <character>, group <character>, your_data <integer/logical>, chi <integer/logical>
  # Identify data in CHI standard table that is not in the dataset ----
  only_chi <- myCHIcomparison[your_data == 0][, list(varname, group)]
  only_chi[varname == 'race3' & group == 'Hispanic', note := "It's OK! A race variable cannot also represent ethnicity."]
  only_chi[varname == 'race3' & group == 'Non-Hispanic', note := "It's OK! A race variable cannot also represent ethnicity."]

  data.table::setorder(only_chi, varname, group)
  formatted_table <- knitr::kable(only_chi,
                                  format = "pipe",
                                  align = c('r', 'l', 'l', 'l'))

  if (nrow(only_chi) > 0){
    message("\U0001f626\U0001f47f\U0001F92C\U00026A0 \n",
            "The following varname and group combinations exist in the rads.data::misc_chi_byvars \n",
            "standards but are missing from your dataset. Please ensure your dataset complies with\n",
            "the CHI standard.\n\n",
            paste(formatted_table, collapse = "\n"), '\n')
  }

  # Identify data in mydata that is not the CHI standard table ----
  only_your_data <- myCHIcomparison[chi == 0]
  only_your_data <- only_your_data[, list(chi_year = rads::format_time(chi_year)), list(varname, group)]
  data.table::setorder(only_your_data, varname, group)
  formatted_table2 <- knitr::kable(only_your_data[, list(chi_year, varname, group)],
                                   format = "pipe",
                                   align = c('r', 'l', 'l'))
  if (nrow(only_your_data) > 0){
    message("\U0001f626\U0001f47f\U0001F92C\U00026A0 \nThe following varname & group combinations in your table are not valid\n",
            "when compared to rads.data::misc_chi_byvars:\n\n",
            paste(formatted_table2, collapse = "\n"), '\n')
  }

  # Give message of success if there are no problems ----
  if (nrow(only_your_data) == 0 && nrow(only_chi) == 0){
    message("\U0001f973\U0001f389\nAll of the CHI variables found in your dataset are formatted according to the standards in rads.data::misc_chi_byvars!\n")
  }

}

## keep_top_8() ----
#' Keep top 8 most frequent categorical values
#'
#' `used by process_sql_server()` & 'process_r_dataframe()'
#'
#' @keywords internal
#' @noRd
#'
#'
keep_top_8 <- function(categorical_freq, config){
  if(nrow(categorical_freq) > 0){
    vals_frequent <- data.table::copy(categorical_freq)
    vals_frequent <- vals_frequent[!is.na(value), rank := data.table::frankv(-count, ties.method = "random"), by = c(config$time_var, 'varname')]
    vals_frequent[is.na(value), rank := 0] # give NA rank of 0 to ensure that it is always selected along with top 8 most frequent
    vals_frequent <- vals_frequent[rank %in% 0:8]
    categorical_freq <- merge(categorical_freq,
                              vals_frequent[, c(config$time_var, 'varname', 'value', 'rank'), with = F],
                              by = c(config$time_var, 'varname', 'value'),
                              all = TRUE)
    categorical_freq[is.na(rank), value := 'Other values']
    categorical_freq <- categorical_freq[, list(count = sum(count, na.rm = TRUE)), by = c(config$time_var, 'varname', 'value')]
    categorical_freq[, proportion := count / sum(count), by = c(config$time_var, 'varname')]
    return(categorical_freq)
  } else { categorical_freq = data.table::data.table()}
}

# Helper functions for SQL query construction ----
## split_column_types() ----
#' Split column types for SQL Server data into categorical, numeric, and date
#'
#' Used by `process_sql_server()`
#'
#' @keywords internal
#' @noRd
#'
#'
split_column_types <- function(config) {
  # Split schema and table name ----
  schema_table <- strsplit(config$data_params$schema_table, "\\.")[[1]]
  schema_name <- schema_table[1]
  table_name <- schema_table[2]

  # Generate query ----
  query <- glue::glue("
    SELECT
      c.name AS varname,
      t.name AS data_type
    FROM
      sys.columns c
    INNER JOIN
      sys.types t ON c.user_type_id = t.user_type_id
    INNER JOIN
      sys.tables tab ON c.object_id = tab.object_id
    INNER JOIN
      sys.schemas s ON tab.schema_id = s.schema_id
    WHERE
      s.name = '{schema_name}' AND tab.name = '{table_name}'
    ORDER BY
      c.column_id
  ")

  # Execute query ----
  result <- data.table::setDT(DBI::dbGetQuery(config$connection, query))

  # Limit to cols in config ----
  result <- result[varname %in% config$data_params$cols]

  # Generate data type ref table ----
  std_data_types <- data.table::data.table(
    data_type = c(
      # Character types
      "char", "varchar", "text", "nchar", "nvarchar", "ntext",
      # Unicode character types
      "unicode_char", "unicode_varchar", "unicode_text",
      # Binary types
      "binary", "varbinary", "image",
      # Number types
      "bit", "tinyint", "smallint", "int", "bigint",
      "decimal", "numeric", "smallmoney", "money",
      "float", "real",
      # Date and datetime types
      "datetime", "datetime2", "smalldatetime", "date", "timestamp",
      # Other types
      "sql_variant", "uniqueidentifier", "xml", "cursor", "table",
      "time", "datetimeoffset"
    ),
    category = c(
      # Character types
      rep("character", 6),
      # Unicode character types
      rep("character", 3),
      # Binary types
      rep("character", 3),
      # Number types
      rep("numeric", 11),
      # Date and Time types
      rep("datetime", 5),
      # Other types
      rep("other", 7)
    )
  )

  # Identify data category ----
  result = merge(result,
                 std_data_types,
                 by = 'data_type',
                 all.x = T, all.y = F)

  if(length(result[category == 'other']$varname) > 0){
    warning(paste0('The following SQL columns will not be processed because they are not clearly numerics or characters:\n',
                   paste(result[category == 'other']$varname, collapse = ', ')))
  }

  # Save distinct categories of columns ----
  config$data_params$cols <- c(result[category == 'character']$varname, result[category == 'numeric']$varname, result[category == 'datetime']$varname)
  config$data_params$cols.character <- result[category == 'character']$varname
  config$data_params$cols.numeric <- result[category == 'numeric']$varname
  config$data_params$cols.datetime <- result[category == 'datetime']$varname

  return(config)
}

## generate_missing_query() ----
#' Generate SQL query to create table of missing data
#'
#' Used by `process_sql_server()`
#'
#' @keywords internal
#' @noRd
#'
generate_missing_query <- function(config) {
  # on 9/16 compared with using simple query for 1 var at a time using future_apply to append. The SQL code below was much faster.
  # Get the column names
  cols <- setdiff(config$data_params$cols, config$time_var)

  # Create a string of column names for the CROSS APPLY VALUES, casting to VARCHAR
  cols_cross_apply <- paste0("('", cols, "', COALESCE(CAST([", cols, "] AS VARCHAR), 'NULL'))", collapse = ", ")

  # Manually quote the time varname
  time_var_quoted <- paste0("[", config$time_var, "]")

  # Construct the SQL query using glue
  query <- glue::glue("
    WITH base_data AS (
      SELECT {time_var_quoted}, {paste0('[', cols, ']', collapse = ', ')}
      FROM {config$data_params$schema_table}
      WHERE {time_var_quoted} BETWEEN {config$time_range[1]} AND {config$time_range[2]}
    )
    SELECT
      {time_var_quoted} AS {config$time_var},
      varname,
      SUM(CASE WHEN value = 'NULL' THEN 1 ELSE 0 END) AS nrow,
      CAST(SUM(CASE WHEN value = 'NULL' THEN 1.0 ELSE 0.0 END) / COUNT(*) AS FLOAT) AS proportion
    FROM (
      SELECT {time_var_quoted}, unpvt.varname, unpvt.value
      FROM base_data
      CROSS APPLY (VALUES {cols_cross_apply}) AS unpvt (varname, value)
    ) AS unpivoted_data
    GROUP BY {time_var_quoted}, varname
    ORDER BY {time_var_quoted}, varname
  ")

  return(query)
}

## generate_numeric_query() ----
#' Generate SQL query for numeric statistics
#'
#' Used by `process_sql_server()`
#'
#' @keywords internal
#' @noRd
#'
generate_numeric_query <- function(config) {
  # Helper function to properly format SQL identifiers
  sql_ident <- function(x) {
    paste0("[", gsub("]", "]]", x), "]")
  }

  # Get the column names
  cols <- setdiff(config$data_params$cols.numeric, config$time_var)

  # Create the column list with type casting in a subquery
  column_list <- paste(sapply(cols, function(col) {
    return(sprintf("CAST(%s AS float) AS %s", sql_ident(col), sql_ident(col)))
  }), collapse = ", ")

  # Create the UNPIVOT column list
  unpivot_list <- paste(sapply(cols, sql_ident), collapse = ", ")

  # Construct the SQL query using glue
  query <- glue::glue("
    WITH casted_data AS (
      SELECT
        {sql_ident(config$time_var)},
        {column_list}
      FROM {config$data_params$schema_table}
    ),
    unpivoted_data AS (
      SELECT
        {sql_ident(config$time_var)},
        unpvt.column_name AS varname,
        unpvt.column_value
      FROM casted_data
      UNPIVOT (
        column_value FOR column_name IN ({unpivot_list})
      ) AS unpvt
      WHERE {sql_ident(config$time_var)} BETWEEN {config$time_range[1]} AND {config$time_range[2]}
        AND unpvt.column_value IS NOT NULL
    ),
    column_stats AS (
      SELECT
        varname,
        COUNT(DISTINCT column_value) AS distinct_count
      FROM unpivoted_data
      GROUP BY varname
    ),
    filtered_columns AS (
      SELECT varname
      FROM column_stats
      WHERE distinct_count >= {config$distinct_threshold}
    ),
    stats AS (
      SELECT
        ud.{sql_ident(config$time_var)},
        ud.varname,
        AVG(CAST(ud.column_value AS FLOAT)) AS mean,
        MIN(ud.column_value) AS min,
        MAX(ud.column_value) AS max,
        COUNT(*) AS row_count,
        COUNT(*) / 2 AS middle_row
      FROM unpivoted_data ud
      JOIN filtered_columns fc ON ud.varname = fc.varname
      GROUP BY ud.{sql_ident(config$time_var)}, ud.varname
    ),
    ordered_data AS (
      SELECT
        ud.{sql_ident(config$time_var)},
        ud.varname,
        ud.column_value,
        ROW_NUMBER() OVER (PARTITION BY ud.{sql_ident(config$time_var)}, ud.varname ORDER BY ud.column_value) AS row_num
      FROM unpivoted_data ud
      JOIN filtered_columns fc ON ud.varname = fc.varname
    ),
    median_calc AS (
      SELECT
        s.{sql_ident(config$time_var)},
        s.varname,
        AVG(CAST(od.column_value AS FLOAT)) AS median
      FROM stats s
      JOIN ordered_data od ON s.{sql_ident(config$time_var)} = od.{sql_ident(config$time_var)} AND s.varname = od.varname
      WHERE od.row_num IN (s.middle_row, s.middle_row + 1)
      GROUP BY s.{sql_ident(config$time_var)}, s.varname
    )
    SELECT
      s.{sql_ident(config$time_var)},
      s.varname,
      s.mean,
      m.median,
      s.min,
      s.max
    FROM stats s
    JOIN median_calc m ON s.{sql_ident(config$time_var)} = m.{sql_ident(config$time_var)} AND s.varname = m.varname
    ORDER BY s.{sql_ident(config$time_var)}, s.varname
  ")

  return(query)
}

## generate_date_query() ----
#' Generate SQL query for date / datetime statistics
#'
#' Used by `process_sql_server()`
#'
#' @keywords internal
#' @noRd
#'
#'
generate_date_query <- function(config) {
  # Helper function to properly format SQL identifiers
  sql_ident <- function(x) {
    paste0("[", gsub("]", "]]", x), "]")
  }

  # Get the column names
  cols <- setdiff(config$data_params$cols.datetime, config$time_var)

  # Create the column list with type casting in a subquery
  column_list <- paste(sapply(cols, function(col) {
    return(sprintf("CAST(%s AS date) AS %s", sql_ident(col), sql_ident(col)))
  }), collapse = ", ")

  # Create the UNPIVOT column list
  unpivot_list <- paste(sapply(cols, sql_ident), collapse = ", ")

  # Construct the SQL query using glue
  query <- glue::glue("
    WITH casted_data AS (
      SELECT
        {sql_ident(config$time_var)},
        {column_list}
      FROM {config$data_params$schema_table}
    ),
    unpivoted_data AS (
      SELECT
        {sql_ident(config$time_var)},
        unpvt.column_name AS varname,
        unpvt.column_value
      FROM casted_data
      UNPIVOT (
        column_value FOR column_name IN ({unpivot_list})
      ) AS unpvt
      WHERE {sql_ident(config$time_var)} BETWEEN {config$time_range[1]} AND {config$time_range[2]}
        AND unpvt.column_value IS NOT NULL
    ),
    column_stats AS (
      SELECT
        varname,
        COUNT(DISTINCT column_value) AS distinct_count
      FROM unpivoted_data
      GROUP BY varname
    ),
    filtered_columns AS (
      SELECT varname
      FROM column_stats
      WHERE distinct_count >= {config$distinct_threshold}
    ),
    stats AS (
      SELECT
        ud.{sql_ident(config$time_var)},
        ud.varname,
        MIN(ud.column_value) AS min,
        MAX(ud.column_value) AS max,
        COUNT(*) AS row_count,
        (COUNT(*) - 1) / 2 AS lower_middle_row,
        COUNT(*) / 2 AS upper_middle_row
      FROM unpivoted_data ud
      JOIN filtered_columns fc ON ud.varname = fc.varname
      GROUP BY ud.{sql_ident(config$time_var)}, ud.varname
    ),
    ordered_data AS (
      SELECT
        ud.{sql_ident(config$time_var)},
        ud.varname,
        ud.column_value,
        ROW_NUMBER() OVER (PARTITION BY ud.{sql_ident(config$time_var)}, ud.varname ORDER BY ud.column_value) AS row_num
      FROM unpivoted_data ud
      JOIN filtered_columns fc ON ud.varname = fc.varname
    ),
    median_calc AS (
      SELECT
        s.{sql_ident(config$time_var)},
        s.varname,
        MAX(CASE WHEN od.row_num = s.lower_middle_row + 1 THEN od.column_value END) AS lower_median,
        MIN(CASE WHEN od.row_num = s.upper_middle_row + 1 THEN od.column_value END) AS upper_median
      FROM stats s
      JOIN ordered_data od ON s.{sql_ident(config$time_var)} = od.{sql_ident(config$time_var)} AND s.varname = od.varname
      WHERE od.row_num IN (s.lower_middle_row + 1, s.upper_middle_row + 1)
      GROUP BY s.{sql_ident(config$time_var)}, s.varname
    )
    SELECT
      s.{sql_ident(config$time_var)},
      s.varname,
      CASE
        WHEN s.row_count % 2 = 0 THEN
          DATEADD(day, DATEDIFF(day, m.lower_median, m.upper_median) / 2, m.lower_median)
        ELSE
          m.upper_median
      END AS median,
      s.min,
      s.max
    FROM stats s
    JOIN median_calc m ON s.{sql_ident(config$time_var)} = m.{sql_ident(config$time_var)} AND s.varname = m.varname
    ORDER BY s.{sql_ident(config$time_var)}, s.varname
  ")

  return(query)
}

## generate_categorical_query() ----
#' Generate SQL query for categorical frequency table
#'
#' Used by `process_sql_server()`
#'
#' @keywords internal
#' @noRd
#'
#'
generate_categorical_query <- function(config) {
  sql_ident <- function(x) {
    paste0("[", gsub("]", "]]", x), "]")
  }

  cols <- setdiff(config$data_params$cols.character, config$time_var)

  cross_apply_cols <- paste(sapply(cols, function(col) {
    glue::glue("'{col}' AS column_name, CASE WHEN {sql_ident(col)} IS NULL THEN 'NULL' ELSE CAST({sql_ident(col)} AS NVARCHAR(MAX)) END AS column_value")
  }), collapse = " UNION ALL SELECT ")

  query <- glue::glue("
    WITH base_data AS (
      SELECT {sql_ident(config$time_var)}, {paste(sapply(cols, sql_ident), collapse = ', ')}
      FROM {config$data_params$schema_table}
      WHERE {sql_ident(config$time_var)} BETWEEN {config$time_range[1]} AND {config$time_range[2]}
    ),
    unpivoted_data AS (
      SELECT
        b.{sql_ident(config$time_var)},
        c.column_name AS varname,
        c.column_value AS value
      FROM base_data b
      CROSS APPLY (SELECT {cross_apply_cols}) c
    )
    SELECT
      {sql_ident(config$time_var)},
      varname,
      value,
      COUNT(*) AS count
    FROM unpivoted_data
    GROUP BY {sql_ident(config$time_var)}, varname, value
    ORDER BY {sql_ident(config$time_var)}, varname, count DESC
  ")

  return(query)
}

#----------------------------- ----
#---- STEP 3: Get tidy results ----
#----------------------------- ----
# etl_qa_final_results() ----
#' @title Final QA results for ETL QA pipeline
#'
#' @description
#' This function processes the initial results from [etl_qa_initial_results()]
#' into a format suitable for reporting and visualization. It is the third step
#' run by [etl_qa_run_pipeline()].
#'
#' @details
#' This is an *internal function* accessible only by use of `:::`, for example,
#' `apde.etl:::etl_qa_final_results(...)`.
#'
#' @param initial_qa_results A list containing the initial QA results.
#' @param config An S3 object of class "qa_data_config" containing configuration settings.
#'
#' @return A list containing formatted and combined results that consists of:
#' - `missingness`: Structured summary of the proportion of missing data per variable and time point
#' - `values`: Combined table with the frequency of categorical variables and simple statistics for numeric and date / datetime variables
#' - `chi_standards`: Comparison of CHI (Community Health Indicator) variables values with those expected based on `rads.data::misc_chi_byvars`
#'
#' @examples
#' \dontrun{
#'
#' # Step 1: generate a config object
#' myconfig <- etl_qa_setup_config(
#'   data_source_type = 'rads',
#'   data_params = list(
#'     function_name = 'get_data_birth',
#'     time_var = 'chi_year',
#'     time_range = c(2021, 2022),
#'     cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city',
#'              'num_prev_cesarean', 'mother_date_of_birth'),
#'     version = 'final',
#'     kingco = FALSE,
#'     check_chi = TRUE
#'   ),
#'   output_directory = 'C:/temp/'
#' )
#'
#' # Step 2: prep the initial results
#' initial_results <- etl_qa_initial_results(config = myconfig)
#'
#' # Step 3: prep the final results
#' final_results <- etl_qa_final_results(initial_qa_results = initial_results,
#'                                       config = myconfig)
#'
#' # Peek at the tables
#' head(final_results$missingness)
#' head(final_results$values)
#' head(final_results$chi_standards)
#'
#' }
#'
#' @keywords internal
#'
#'
etl_qa_final_results <- function(initial_qa_results, config) {
  # Extract the time variable name from the config ----
  time_var <- config$time_var

  # Process missing data ----
  missing_data <- initial_qa_results$missing_data
  if (!is.null(missing_data)) {
    data.table::setnames(missing_data, time_var, "time_period")
    missing_data[, abs_change := data.table::fifelse(
      abs((proportion - data.table::shift(proportion)) * 100) > config$abs_threshold,
      paste0(round((proportion - data.table::shift(proportion)) * 100, 1), "%"),
      NA_character_
    ), by = list(varname)]
    data.table::setorder(missing_data, varname, time_period)
    missing_data[, proportion := rads::round2(proportion, config$digits_prop)]
  }

  # Process categorical data ----
  vals_categorical <- initial_qa_results$vals_categorical
  if (nrow(vals_categorical) > 0) {
    if (!is.null(vals_categorical)) {

      # first create a template of all possible unique varname x value x time_period
      unique_values <- unique(vals_categorical[, list(varname, value)])
      unique_values <- unique_values[, c(.SD, stats::setNames(list(config$time_range[1]:config$time_range[2]), config$time_var)),
                                 by = list(varname, value)]

      # then merge the actual data onto to the template
      vals_categorical <- merge(
        unique_values,
        vals_categorical,
        by = c(config$time_var, 'varname', 'value'),
        all = T)
      vals_categorical[is.na(count), count := 0]
      vals_categorical[is.na(proportion), proportion := 0]

      data.table::setnames(vals_categorical, time_var, "time_period")
      vals_categorical[, abs_proportion_change := data.table::fifelse(
        abs((proportion - data.table::shift(proportion)) * 100) > config$abs_threshold,
        paste0(round((proportion - data.table::shift(proportion)) * 100, 1), "%"),
        NA_character_
      ), by = list(varname, value)]
      vals_categorical[, proportion := rads::round2(proportion, config$digits_prop)]
    }
  }

  # Process continuous data ----
  vals_continuous <- initial_qa_results$vals_continuous
  if (nrow(vals_continuous) > 0) {
    if (!is.null(vals_continuous)) {

      vals_continuous <- merge(
        data.table::setnames(data.table::CJ(config$time_range[1]:config$time_range[2], unique(vals_continuous$varname)), c(config$time_var, 'varname')),
        vals_continuous,
        by = c(config$time_var, 'varname'),
        all = T)

      data.table::setnames(vals_continuous, time_var, "time_period")
      vals_continuous[, `:=`(
        rel_mean_change = data.table::fifelse(
          abs((mean / data.table::shift(mean) - 1) * 100) > config$rel_threshold,
          paste0(round((mean / data.table::shift(mean) - 1) * 100, 1), "%"),
          NA_character_
        ),
        rel_median_change = data.table::fifelse(
          abs((median / data.table::shift(median) - 1) * 100) > config$rel_threshold,
          paste0(round((median / data.table::shift(median) - 1) * 100, 1), "%"),
          NA_character_
        )
      ), by = list(varname)]
      vals_continuous[, mean := rads::round2(mean, config$digits_mean)]
      vals_continuous[, median := rads::round2(median, config$digits_mean)]
      vals_continuous[, min := rads::round2(min, config$digits_mean)]
      vals_continuous[, max := rads::round2(max, config$digits_mean)]
    }
  }

  # Process date data ----
  vals_date <- initial_qa_results$vals_date
  if (nrow(vals_date) > 0) {

    vals_date <- merge(
      data.table::setnames(data.table::CJ(config$time_range[1]:config$time_range[2], unique(vals_date$varname)), c(config$time_var, 'varname')),
      vals_date,
      by = c(config$time_var, 'varname'),
      all = T)

    data.table::setnames(vals_date, time_var, "time_period")
    data.table::setnames(vals_date, c('median', 'min', 'max'), c('median_date', 'min_date', 'max_date')) # need to change names because otherwise date format would be lost when combined with numeric data
  }

  # Process CHI comparison data ----
  chi_standards <- initial_qa_results$chi_standards
  if (nrow(chi_standards) > 0){
    chi_standards[your_data == 0 | chi == 0, problem := '*']
  }

  # Combined categorical, continuous, and date summaries ----
  values <- rbind(
    vals_categorical[, vartype := 'Categorical'],
    vals_continuous[, vartype := 'Continuous'],
    vals_date[, vartype := 'Date'],
    fill = T
  )
  keepvars <- intersect(
    names(values),
    c('time_var' = time_var, 'time_period', 'vartype', 'varname', 'value', 'mean', 'median', 'min', 'max', 'median_date', 'min_date', 'max_date', 'count', 'proportion', 'abs_proportion_change', 'rel_mean_change', 'rel_median_change')
  )

  values <- values[, keepvars, with = FALSE]
  data.table::setorderv(values, intersect(c('varname', 'value', 'time_period'), names(values)))

  # Return results as a list ----
  return(list(
    missingness = data.table::setkey(missing_data, NULL),
    values = data.table::setkey(values, NULL),
    chi_standards = data.table::setkey(chi_standards, NULL)
  ))
}

#------------------------------------------------- ----
#---- STEP 4: Export tables & graphs of QA results ----
#------------------------------------------------- ----
# etl_qa_export_results() ... main function ----
#' @title Export tables and graphs of ETL QA pipeline results
#'
#' @description
#' This function exports Excel tables and PDF plots of ETL QA results. It is the
#' fourth and final step run by [etl_qa_run_pipeline()].
#'
#' @details
#' This is an *internal function* accessible only by use of `:::`, for example,
#' `apde.etl:::etl_qa_export_results(...)`.
#'
#' @param qa_results A list containing the processed QA results from `etl_qa_final_results()`
#' @param config An S3 object of class "qa_data_config" containing configuration settings.
#'
#' @return A list the file paths for the exported data:
#' - `pdf_missing`: File path to PDF of plots of data missingness over time
#' - `pdf_values`: File path to PDF of plots of frequency and statistical changes over time
#' - `excel`: File path to Excel file with tabs for missingness, values, CHI comparisons
#'
#' @examples
#' \dontrun{
#'
#' # Step 1: generate a config object
#' myconfig <- etl_qa_setup_config(
#'   data_source_type = 'rads',
#'   data_params = list(
#'     function_name = 'get_data_birth',
#'     time_var = 'chi_year',
#'     time_range = c(2021, 2022),
#'     cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city',
#'              'num_prev_cesarean', 'mother_date_of_birth'),
#'     version = 'final',
#'     kingco = FALSE,
#'     check_chi = TRUE
#'   ),
#'   output_directory = 'C:/temp/'
#' )
#'
#' # Step 2: prep the initial results
#' initial_results <- etl_qa_initial_results(config = myconfig)
#'
#' # Step 3: prep the final results
#' final_results <- etl_qa_final_results(initial_qa_results = initial_results,
#'                                       config = myconfig)
#'
#' # Step 4: Export tables and visualizations
#' etl_qa_export_results(qa_results = final_results, config = myconfig)
#'
#' }
#'
#' @keywords internal
#'
#'
etl_qa_export_results <- function(qa_results, config) {
  # Extract information from config ----
  output_directory <- config$output_directory
  time_var <- config$time_var

  # Identify the data source ----
  if(config$data_source_type == 'sql_server'){
    datasource = config$data_params$schema_table
  } else if (config$data_source_type == 'rads') {
    datasource = paste0('rads_', config$data_params$function_name, '_(', config$data_params$version, ')')
  } else {
    datasource = config$data_params$data_source
  }

  # Warning when a column is missing 100% of the time ----
  mi100 <- data.table::copy(qa_results$missingness)[, list(all_proportion_one = all(proportion == 1)), by = varname][all_proportion_one == TRUE]
  if(nrow(mi100) > 0){
    mi100vars <- paste0(unique(mi100$varname), collapse  = ', ') # string of all 100% missing separated by comma
    mi100vars <- sub(", ([^,]*)$", " & \\1", mi100vars) # replace last comma with ampersand
    warning("\n\U00026A0\nThe following variables are 100% missing across all time points and therefore DO NOT have value plots:\n",
            mi100vars, immediate.=TRUE)
    mi100vars <- unique(mi100$varname) # save a clean vector of all variables with 100% missing data
  } else {mi100vars <- c() }

  # Function to create plots ----
  create_plots <- function(plot_data, plot_type) {
    plot_data <- plot_data[!is.na(varname)] # arises where there were no date variables, or no continuous variables, etc.

    pdf_file <- file.path(output_directory, paste0(datasource, '_qa_', plot_type, '_', gsub("-", "_", Sys.Date()), '.pdf'))
    grDevices::pdf(pdf_file, onefile = TRUE, width = 11, height = 8.5)

    mytitle <- paste0('Data QA ', plot_type, ': ',
                      datasource, ' ',
                      format(Sys.Date(), "%B %d, %Y"))

    if (plot_type == "missing") {
      plots <- plotMISSING(plot_data, time_var, mytitle)
      for (plot in plots) {
        print(plot)
      }
    } else if (plot_type == "values") {
      for (var in setdiff(unique(plot_data$varname), mi100vars)) { # exclude vars that are 100% missing at all time points
        # message('Plotting ', var)
        var_data <- plot_data[varname == var]
        if (all(var_data$vartype == 'Categorical')) {
          myplot <- plotCATEGORICAL(var_data, time_var, mytitle)
        } else if (all(var_data$vartype == 'Continuous')) {
          myplot <- plotCONTINUOUS(var_data, time_var, mytitle)
        } else if (all(var_data$vartype == 'Date')) {
          myplot <- plotDATE(var_data, time_var, mytitle)
        }
        print(myplot)
      }
    }

    grDevices::dev.off()
  }

  # Create plots for missing data and values ----
  create_plots(qa_results$missingness, "missing")
  create_plots(qa_results$values, "values")

  # Write to Excel ----
  excel_filepath <- file.path(output_directory, paste0(datasource, '_qa_', gsub("-", "_", Sys.Date()), '.xlsx'))
  wb <- openxlsx::createWorkbook()

  openxlsx::addWorksheet(wb, "missingness")
  openxlsx::writeDataTable(wb, "missingness", qa_results$missingness, rowNames = FALSE)

  openxlsx::addWorksheet(wb, "values")
  openxlsx::writeDataTable(wb, "values", qa_results$values, rowNames = FALSE)

  if(nrow(qa_results$chi_standards) > 0){
    openxlsx::addWorksheet(wb, "CHI_standards")
    openxlsx::writeDataTable(wb, "CHI_standards", qa_results$chi_standards, rowNames = FALSE)
  }

  openxlsx::saveWorkbook(wb, excel_filepath, overwrite = TRUE)

  # Return file paths ----
  message("Finished! Check your output_directory: ", output_directory)

  return(list(
    pdf_missing = file.path(output_directory, paste0(datasource, '_qa_missing_', gsub("-", "_", Sys.Date()), '.pdf')),
    pdf_values = file.path(output_directory, paste0(datasource, '_qa_values_', gsub("-", "_", Sys.Date()), '.pdf')),
    excel = excel_filepath
  ))
}

# Helper functions for plotting ----
## plotCATEGORICAL() ----
#' Plot categorical data
#'
#' Used by `etl_qa_export_results()`
#'
#' @keywords internal
#' @noRd
#'
#'
plotCATEGORICAL <- function(var_data, time_var, mytitle) {
  value_levels <- levels(factor(var_data$value, exclude = NULL))
  linetypes <- rep("solid", length(value_levels))
  names(linetypes) <- value_levels
  linetypes[is.na(names(linetypes))] <- "dotted"

  ggplot2::ggplot(var_data, ggplot2::aes(x = time_period, y = proportion, color = value, linetype = value)) +
    ggplot2::geom_line(ggplot2::aes(linewidth = ifelse(is.na(value), 1.5, 2))) +
    ggplot2::scale_x_continuous(name = time_var, breaks = seq(min(var_data[['time_period']]), max(var_data[['time_period']]), length.out = 5)) +
    ggplot2::scale_y_continuous(limits = c(0, 1)) +
    ggplot2::scale_color_manual(values = c(scales::hue_pal()(length(unique(stats::na.omit(var_data$value)))), "black"),
                       na.value = "black") +
    ggplot2::scale_linetype_manual(values = c(rep("solid", length(unique(stats::na.omit(var_data$value)))), "dotted"),
                          na.value = "dotted") +
    ggplot2::scale_linewidth_identity() +
    ggplot2::labs(title = mytitle,
         subtitle = paste0('', var_data$varname[1]),
         x = time_var,
         y = 'Proportion',
         color = 'Unique Values') +
    ggplot2::theme_bw() +
    ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5),
          plot.subtitle = ggplot2::element_text(hjust = 0.5, face = 'bold', size = 16)) +
    ggplot2::guides(linetype = "none", linewidth = "none",
           color = ggplot2::guide_legend(override.aes = list(linetype = linetypes)))
}

## plotCONTINUOUS() ----
#' Plot continuous data
#'
#' Used by `etl_qa_export_results()`
#'
#' @keywords internal
#' @noRd
#'
plotCONTINUOUS <- function(var_data, time_var, mytitle) {
  # Typical case where there is more than 1 row of data
  if(nrow(var_data[!is.na(mean)]) > 1){
    plot <- ggplot2::ggplot(var_data) +
      ggplot2::geom_line(ggplot2::aes(x = time_period, y = min, color = "Minimum", linetype = "Minimum"), linewidth = 2) +
      ggplot2::geom_line(ggplot2::aes(x = time_period, y = mean, color = "Mean", linetype = "Mean"), linewidth = 1.5) +
      ggplot2::geom_line(ggplot2::aes(x = time_period, y = median, color = "Median", linetype = "Median"), linewidth = 1.5) +
      ggplot2::geom_line(ggplot2::aes(x = time_period, y = max, color = "Maximum", linetype = "Maximum"), linewidth = 2) +
      ggplot2::scale_linetype_manual(name = "Stats",
                            values = c("Minimum" = "solid",
                                       "Mean" = "dotted",
                                       "Median" = "1212",
                                       "Maximum" = "solid"))

  } else if (nrow(var_data[!is.na(mean)]) == 1) {
    plot <- ggplot2::ggplot(var_data) +
      ggplot2::geom_point(ggplot2::aes(x = time_period, y = min, color = "Minimum"), size = 3) +
      ggplot2::geom_point(ggplot2::aes(x = time_period, y = mean, color = "Mean"), size = 3) +
      ggplot2::geom_point(ggplot2::aes(x = time_period, y = median, color = "Median"), size = 3) +
      ggplot2::geom_point(ggplot2::aes(x = time_period, y = max, color = "Maximum"), size = 3)
  }

  plot <- plot +
    ggplot2::scale_color_manual(name = "Stats",
                       values = c("Minimum" = "#2C7BB6",
                                  "Mean" = "#D7191C",
                                  "Median" = "#ABDDA4",
                                  "Maximum" = "#FDAE61")) +

    ggplot2::scale_x_continuous(name = time_var, breaks = seq(min(var_data[['time_period']]), max(var_data[['time_period']]), length.out = 5)) +
    ggplot2::labs(title = mytitle, subtitle = paste0('', var_data$varname[1]), x = time_var, y = var_data$varname[1]) +
    ggplot2::theme_bw() +
    ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5),
          plot.subtitle = ggplot2::element_text(hjust = 0.5, face = 'bold', size = 16)) +
    ggplot2::guides(color = ggplot2::guide_legend(override.aes = list(linetype = c("solid", "dotted", "1212", "solid"))))

  return(plot)
}

## plotDATE() ----
#' Plot date data
#'
#' Used by `etl_qa_export_results()`
#'
#' @keywords internal
#' @noRd
#'
#'
plotDATE <- function(var_data, time_var, mytitle) {
  # Typical case where there is more than 1 row of data
  if (nrow(var_data[!is.na(median_date)]) > 1){
    plot <-   ggplot2::ggplot(var_data[!is.na(median_date)]) +
      ggplot2::geom_line(ggplot2::aes(x = time_period, y = min_date, color = "Minimum", linetype = "Minimum"), linewidth = 2) +
      ggplot2::geom_line(ggplot2::aes(x = time_period, y = median_date, color = "Median", linetype = "Median"), linewidth = 1.5) +
      ggplot2::geom_line(ggplot2::aes(x = time_period, y = max_date, color = "Maximum", linetype = "Maximum"), linewidth = 2) +
      ggplot2::scale_linetype_manual(name = "Stats",
                            values = c("Minimum" = "solid",
                                       "Median" = "1212",
                                       "Maximum" = "solid"))
  } else if (nrow(var_data[!is.na(median_date)]) == 1){
    plot <-   ggplot2::ggplot(var_data[!is.na(median_date)]) +
      ggplot2::geom_point(ggplot2::aes(x = time_period, y = min_date, color = "Minimum"), size = 3) +
      ggplot2::geom_point(ggplot2::aes(x = time_period, y = median_date, color = "Median"), size = 3) +
      ggplot2::geom_point(ggplot2::aes(x = time_period, y = max_date, color = "Maximum"), size = 3)
  }

  plot <- plot +
    ggplot2::scale_color_manual(name = "Stats",
                       values = c("Minimum" = "#2C7BB6",
                                  "Median" = "#ABDDA4",
                                  "Maximum" = "#FDAE61")) +
    ggplot2::scale_x_continuous(name = time_var, breaks = seq(min(var_data[['time_period']]), max(var_data[['time_period']]), length.out = 5)) +
    ggplot2::scale_y_date(date_labels = "%Y-%m-%d",
                 limits = c(min(var_data$min_date), max(var_data$max_date)),
                 # date_breaks = "5 year" # commented out because never know the scale of dates being assessed
    ) +
    ggplot2::labs(title = mytitle, subtitle = paste0('', var_data$varname[1]), x = time_var, y = var_data$varname[1]) +
    ggplot2::theme_bw() +
    ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5),
          plot.subtitle = ggplot2::element_text(hjust = 0.5, face = 'bold', size = 16)) +
    ggplot2::guides(color = ggplot2::guide_legend(override.aes = list(linetype = c("solid", "1212", "solid"))))

  return(plot)
}

## plotMISSING() ----
#' Plot missing data
#'
#' Used by `etl_qa_export_results()`
#'
#' @keywords internal
#' @noRd
#'
#'
plotMISSING <- function(plot_data, time_var, mytitle) {
  plot_data[, vargroup := ceiling(as.numeric(factor(varname)) / 16)]
  plots <- list()
  for (ii in unique(plot_data$vargroup)) {
    myplot <- ggplot2::ggplot(plot_data[vargroup == ii],
                     ggplot2::aes(y = proportion, x = time_period, color = varname)) +
      ggplot2::geom_point(size = 2.5) +
      ggplot2::geom_line(linewidth = 2) +
      ggplot2::facet_wrap('varname', ncol = 4) +
      ggplot2::scale_x_continuous(name = time_var, breaks = seq(min(plot_data[['time_period']]), max(plot_data[['time_period']]), length.out = 5)) +
      ggplot2::scale_y_continuous(limits = c(0, 1), labels = scales::percent_format(accuracy = 1L)) +
      ggplot2::ylab('Percent missing') +
      ggplot2::ggtitle(mytitle) +
      ggplot2::theme_bw() +
      ggplot2::theme(legend.position = 'none')
    plots[[ii]] <- myplot
  }
  return(plots)
}
