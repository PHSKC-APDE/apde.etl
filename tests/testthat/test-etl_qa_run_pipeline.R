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
  fake_data_function <- function(...) {
    captured <<- list(...)
    stop("stop after capturing arguments")
  }

  # mock apde.data so no database or network access is needed
  local_mocked_bindings(
    list_data_columns = function(...) data.table::data.table(var.names = c('chi_year', 'chi_age', 'chi_geo_kc')),
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

  # chars() & death() do not have a version argument, so it must not be passed
  for (fn in c('chars', 'death')) {
    captured <- NULL
    expect_error(process_apde_data(make_config(fn)), "stop after capturing")
    expect_false('version' %in% names(captured), info = fn)
    expect_equal(captured$year, 2020:2021, info = fn)
  }
})
