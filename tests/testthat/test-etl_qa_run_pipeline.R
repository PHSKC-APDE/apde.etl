# etl_qa_apde_data_functions() ----
test_that("etl_qa_apde_data_functions lists valid apde.data functions", {
  fns <- etl_qa_apde_data_functions()

  expect_true(all(c('birth', 'brfss', 'chars', 'death', 'infant_death') %in% fns$all))
  expect_false('population' %in% fns$all)
  expect_setequal(fns$versioned, c('birth', 'brfss'))
  expect_true(all(fns$versioned %in% fns$all))

  # every valid function_name must be an actual apde.data function with the arguments the pipeline uses
  for (fn in fns$all) {
    args <- names(formals(utils::getFromNamespace(fn, 'apde.data')))
    expect_true(all(c('cols', 'year', 'kingco') %in% args), info = fn)
  }

  # and only the 'versioned' functions should have a version argument that is stage / final
  for (fn in fns$versioned) {
    expect_true('version' %in% names(formals(utils::getFromNamespace(fn, 'apde.data'))), info = fn)
  }
})

# etl_qa_setup_config() ----
test_that("etl_qa_setup_config only keeps `version` for apde.data functions that have one", {
  params <- list(time_var = 'chi_year', time_range = c(2020, 2021), cols = 'chi_age', version = 'final')

  cfg_birth <- etl_qa_setup_config(data_source_type = 'apde.data', output_directory = tempdir(),
                                   data_params = c(params, function_name = 'birth'))
  expect_equal(cfg_birth$data_params$version, 'final')

  cfg_chars <- etl_qa_setup_config(data_source_type = 'apde.data', output_directory = tempdir(),
                                   data_params = c(params, function_name = 'chars'))
  expect_null(cfg_chars$data_params$version)
})

test_that("etl_qa_setup_config validates function_name", {
  params <- list(time_var = 'chi_year', time_range = c(2020, 2021), cols = 'chi_age')

  expect_error(etl_qa_setup_config(data_source_type = 'apde.data', output_directory = tempdir(),
                                   data_params = c(params, function_name = 'population')),
               "valid 'function_name'")
  expect_error(etl_qa_setup_config(data_source_type = 'apde.data', output_directory = tempdir(),
                                   data_params = c(params, function_name = 'get_data_birth')),
               "valid 'function_name'")
  expect_no_error(etl_qa_setup_config(data_source_type = 'apde.data', output_directory = tempdir(),
                                      data_params = c(params, function_name = 'infant_death')))
})

# etl_qa_run_pipeline() ----
test_that("etl_qa_run_pipeline validates function_name", {
  params <- list(time_var = 'chi_year', time_range = c(2020, 2021), cols = 'chi_age')

  expect_error(etl_qa_run_pipeline(data_source_type = 'apde.data',
                                   data_params = c(params, function_name = 'population'),
                                   output_directory = tempdir()),
               "function_name you provided is invalid")
  expect_error(etl_qa_run_pipeline(data_source_type = 'rads',
                                   data_params = c(params, function_name = 'birth'),
                                   output_directory = tempdir()),
               "must be one of")
})

# process_apde_data() ----
test_that("process_apde_data only passes `version` to apde.data functions that have it", {
  captured <- NULL
  captured_years_args <- NULL
  fake_data_function <- function(...) {
    captured <<- list(...)
    stop("stop after capturing arguments")
  }

  # mock apde.data so no database or network access is needed
  local_mocked_bindings(
    list_data_columns = function(...) data.table::data.table(var.names = c('chi_year', 'chi_age', 'chi_geo_kc')),
    list_data_years = function(...) {
      captured_years_args <<- list(...)
      2015:2025
    },
    birth = fake_data_function,
    chars = fake_data_function,
    death = fake_data_function,
    .package = 'apde.data'
  )

  make_config <- function(fn) {
    etl_qa_setup_config(data_source_type = 'apde.data', output_directory = tempdir(),
                        data_params = list(function_name = fn, time_var = 'chi_year',
                                           time_range = c(2020, 2021), cols = 'chi_age',
                                           version = 'final', kingco = TRUE))
  }

  # birth() has stage / final versions, so version is passed
  expect_error(process_apde_data(make_config('birth')), "stop after capturing")
  expect_equal(captured$version, 'final')
  expect_equal(captured$year, 2020:2021)
  expect_true(captured$kingco)
  expect_equal(captured_years_args$version, 'final') # years come from the same version as the data

  # chars() & death() do not have a version argument, so it must not be passed
  for (fn in c('chars', 'death')) {
    captured <- NULL
    expect_error(process_apde_data(make_config(fn)), "stop after capturing")
    expect_false('version' %in% names(captured), info = fn)
    expect_equal(captured$year, 2020:2021, info = fn)
    expect_null(captured_years_args$version, info = fn)
  }
})

test_that("process_apde_data only requests columns that exist in the requested years", {
  captured_cols_year <- NULL
  captured <- NULL

  # mock apde.data: 'zipcode' only exists in 2025, mimicking HYS
  local_mocked_bindings(
    list_data_columns = function(dataset, year = NULL, ...) {
      captured_cols_year <<- year
      vars <- c('chi_year', 'chi_sex', 'chi_geo_kc')
      if (is.null(year) || 2025 %in% year) vars <- c(vars, 'zipcode')
      data.table::data.table(var.names = vars)
    },
    hys = function(...) {
      captured <<- list(...)
      stop("stop after capturing arguments")
    },
    list_data_years = function(...) c(2019L, 2021L, 2023L, 2025L),
    .package = 'apde.data'
  )

  config <- etl_qa_setup_config(data_source_type = 'apde.data', output_directory = tempdir(),
                                data_params = list(function_name = 'hys', time_var = 'chi_year',
                                                   time_range = c(2023, 2023), cols = 'chi_sex',
                                                   kingco = FALSE, check_chi = TRUE))

  expect_error(process_apde_data(config), "stop after capturing")
  expect_equal(captured_cols_year, 2023L)
  expect_equal(captured$year, 2023L)
  expect_false('zipcode' %in% captured$cols) # zipcode is a CHI standard var but absent in 2023
  expect_true('chi_sex' %in% captured$cols)
})

test_that("process_apde_data drops HYS years that have no data", {
  captured_cols_year <- NULL
  captured <- NULL

  local_mocked_bindings(
    list_data_columns = function(dataset, year = NULL, ...) {
      captured_cols_year <<- year
      data.table::data.table(var.names = c('chi_year', 'chi_sex', 'chi_geo_kc'))
    },
    hys = function(...) {
      captured <<- list(...)
      stop("stop after capturing arguments")
    },
    list_data_years = function(...) c(2019L, 2021L, 2023L, 2025L),
    .package = 'apde.data'
  )

  make_config <- function(time_range) {
    etl_qa_setup_config(data_source_type = 'apde.data', output_directory = tempdir(),
                        data_params = list(function_name = 'hys', time_var = 'chi_year',
                                           time_range = time_range, cols = 'chi_sex',
                                           kingco = FALSE, check_chi = FALSE))
  }

  # off years are dropped, with a message, before both the column check and the data pull
  expect_message(
    expect_error(process_apde_data(make_config(c(2019, 2023))), "stop after capturing"),
    "not available for: 2020, 2022"
  )
  expect_equal(captured_cols_year, c(2019L, 2021L, 2023L))
  expect_equal(captured$year, c(2019L, 2021L, 2023L))

  # a range with no HYS years at all gives a clear error
  expect_error(process_apde_data(make_config(c(2020, 2020))), "No hys data are available")
})

test_that("process_apde_data does not filter PUMS years", {
  captured <- NULL

  local_mocked_bindings(
    list_data_columns = function(...) data.table::data.table(var.names = c('chi_year', 'agep', 'chi_geo_kc')),
    list_data_years = function(...) stop("list_data_years() should not be used for PUMS"),
    pums = function(...) {
      captured <<- list(...)
      stop("stop after capturing arguments")
    },
    .package = 'apde.data'
  )

  config <- etl_qa_setup_config(data_source_type = 'apde.data', output_directory = tempdir(),
                                data_params = list(function_name = 'pums', time_var = 'chi_year',
                                                   time_range = c(2018, 2022), cols = 'agep',
                                                   kingco = TRUE, check_chi = FALSE))

  # a 5-year PUMS period includes 2020, so all 5 years must be passed to pums()
  expect_error(process_apde_data(config), "stop after capturing")
  expect_equal(captured$year, 2018:2022)
})
