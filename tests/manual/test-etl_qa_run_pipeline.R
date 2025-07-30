library(rads)
library(data.table)
library(DBI)

# Notes ----
# Tests should usually be self contained, but the nature of these functions 
# necessitates testing with our infrastructure

# For efficiency, will only test the highest level function since it calls on all 
# sub functions. 

# Set up directories, connections, data, and estimates for testing below ----
  ## Create a temporary directory to save output ----
    myOutputFolder <- tempdir()
  
  ## Create connections ----
    myconnection <- rads::validate_hhsaw_key()

    myNEWconnection <- rads::validate_hhsaw_key()
    DBI::dbDisconnect(myNEWconnection) # disconnected DBIconnection needed to throw the error message below
    
  ## Create synthetic data & estimates for testing numeric arguments ----
    set.seed(98104)
    synthetic_data <- data.table(
      myyear = sample(2011:2020, size = 10000, replace = T), 
      mycategorical = factor(sample(c('alpha', 'beta', 'gamma', 'delta'), size = 10000, replace = T)), 
      myinteger = rnorm(10000, mean = 5000, sd = 300))
    synthetic_data[sample(200), mycategorical := NA]
    synthetic_data[sample(which(synthetic_data$myyear == 2016), 40), mycategorical := NA] # extra missing for 2016
    synthetic_data[sample(350), myinteger := NA]
    
    synthetic_1 <- etl_qa_run_pipeline(
      data_source_type = 'r_dataframe',
      data_params = list(
        data = synthetic_data,
        time_var = 'myyear',
        time_range = c(2011, 2020),
        cols = c('mycategorical', 'myinteger'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder, 
      digits_mean = 0, 
      digits_prop = 3, 
      abs_threshold = 3, 
      rel_threshold = 2
    )
    
    synthetic_2 <- etl_qa_run_pipeline(
      data_source_type = 'r_dataframe',
      data_params = list(
        data = synthetic_data,
        time_var = 'myyear',
        time_range = c(2011, 2020),
        cols = c('mycategorical', 'myinteger'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder, 
      digits_mean = 3, 
      digits_prop = 5, 
      abs_threshold = 1, 
      rel_threshold = 0
    )
  
  ## Generic run with RADS ----
    qa.rads <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city', 
                 'num_prev_cesarean', 'mother_date_of_birth'),
        version = 'final', 
        kingco = FALSE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
  
  ## Generic run with R dataframe ----
    birth_data <- rads::get_data_birth(year = c(2021:2022), 
                                       kingco = F, 
                                       cols = c('chi_age', 'race4', 'birth_weight_grams', 
                                                'birthplace_city', 'num_prev_cesarean', 
                                                'chi_year', 'mother_date_of_birth'), 
    )
    qa.df <- etl_qa_run_pipeline(
      data_source_type = 'r_dataframe',
      data_params = list(
        data = birth_data,
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city', 
                 'num_prev_cesarean', 'mother_date_of_birth'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )

  ## Generic run with SQL Server ----
    qa.sql <- etl_qa_run_pipeline(
      data_source_type = 'sql_server',
      connection = myconnection,
      data_params = list(
        schema_table = 'birth.final_analytic',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols =c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city', 
                'num_prev_cesarean', 'mother_date_of_birth'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
  
  ## Generic run with SQL Server & check_chi = TRUE ----
    qa.sqlchi <- etl_qa_run_pipeline(
      data_source_type = 'sql_server',
      connection = myconnection,
      data_params = list(
        schema_table = 'birth.final_analytic',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols =c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city', 
                'num_prev_cesarean', 'mother_date_of_birth'), 
        check_chi = TRUE
      ), 
      output_directory = myOutputFolder
    )  
  
# test etl_qa_run_pipeline functionality ----
  test_that("Identical names of objects in the returned result list", {
    expect_identical(names(qa.rads), names(qa.df)) 
    expect_identical(names(qa.rads), names(qa.sql)) 
  })  
  
  test_that("Results are identical regardless of method of accessing data", {
    expect_identical(qa.rads$final, qa.df$final)
    expect_identical(qa.rads$final, qa.sql$final)
  })
  
  test_that("1 Excel file and 2 PDF files were exported", {
    expect_true(file.exists(qa.rads$exported$pdf_missing))
    expect_true(file.exists(qa.rads$exported$pdf_values))
    expect_true(file.exists(qa.rads$exported$excel))
    
    expect_true(file.exists(qa.df$exported$pdf_missing))
    expect_true(file.exists(qa.df$exported$pdf_values))
    expect_true(file.exists(qa.df$exported$excel))
    
    expect_true(file.exists(qa.sql$exported$pdf_missing))
    expect_true(file.exists(qa.sql$exported$pdf_values))
    expect_true(file.exists(qa.sql$exported$excel))
  })
  
  test_that('chi_check = TRUE works as expected', {
    # all specified cols are still there when use chi_check = TRUE
    expect_true(all(qa.sql$final$missingness$varname %in% qa.sqlchi$final$missingness$varname))
    
    # chi_check = TRUE returns additional cols, at least 20 of which begin with 'chi'
    expect_gt(sum(grepl('^chi_', setdiff(unique(qa.sqlchi$final$missingness$varname), unique(qa.sql$final$missingness$varname)))), 
              20)
  })
  
  test_that('Ensure `kingco` = TRUE works as expected', {
    KCfalse <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('chi_geo_kc'),
        version = 'final', 
        kingco = FALSE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    
    KCtrue <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('chi_geo_kc'),
        version = 'final', 
        kingco = TRUE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    
    expect_gt(sum(KCfalse$final$values$count, na.rm = T), sum(KCtrue$final$values$count, na.rm = T))
  })
  
  test_that('Ensure processing a single numeric, or character, or date var at a time does not cause problems', {
    # test for both rads and SQL, to effectively test all options since rads uses the data.frame/data.table code
    # character only ----
    qa.rads.char <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('chi_geo_kc'),
        version = 'final', 
        kingco = FALSE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    
    expect_identical(names(qa.rads.char), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.rads.char$final$values), 6) # two rows for chi_geo_KC == 'King County' and two rows where is.na(chi_geo_kc) and empty rows for continous and dates
    
    qa.sql.char <- etl_qa_run_pipeline(
      data_source_type = 'sql_server',
      connection = myconnection,
      data_params = list(
        schema_table = 'birth.final_analytic',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols =c('chi_geo_kc'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    expect_identical(names(qa.sql.char), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.sql.char$final$values), 6)
    
    # continuous only ----
    qa.rads.cont <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('birth_weight_grams'),
        version = 'final', 
        kingco = FALSE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    
    expect_identical(names(qa.rads.cont), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.rads.cont$final$values), 4) # two years for continuous plus emptry rows for character and date
    
    qa.sql.cont <- etl_qa_run_pipeline(
      data_source_type = 'sql_server',
      connection = myconnection,
      data_params = list(
        schema_table = 'birth.final_analytic',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols =c('birth_weight_grams'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    expect_identical(names(qa.sql.cont), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.sql.cont$final$values), 4)
    
    # date only ----
    qa.rads.date <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('mother_date_of_birth'),
        version = 'final', 
        kingco = FALSE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    
    expect_identical(names(qa.rads.date), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.rads.date$final$values), 4) # two years for dates plus empty row for categorical and continous
    
    qa.sql.date <- etl_qa_run_pipeline(
      data_source_type = 'sql_server',
      connection = myconnection,
      data_params = list(
        schema_table = 'birth.final_analytic',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols =c('mother_date_of_birth'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    expect_identical(names(qa.sql.date), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.sql.date$final$values), 4)
  })
  
  test_that('Ensure `digits_prop` rounds proportion as expected', {
    # rounding to 3 digits (default)
    expect_equal(synthetic_1$final$missingness$proportion, round(synthetic_1$final$missingness$proportion, 3))
    expect_equal(synthetic_1$final$values$proportion, rads::round2(synthetic_1$final$value$proportion, 3))
    
    # rounding to 5 digits
    expect_equal(synthetic_2$final$missingness$proportion, round(synthetic_2$final$missingness$proportion, 5))
    expect_equal(synthetic_2$final$values$proportion, rads::round2(synthetic_2$final$value$proportion, 5))            
  })

  test_that('Ensure `digits_mean` rounds mean, median, min, & max as expected', {
    # rounding to 0 digits (default)
    expect_equal(synthetic_1$final$values[!is.na(mean)]$mean, rads::round2(synthetic_1$final$values[!is.na(mean)]$mean, 0))
    expect_equal(synthetic_1$final$values[!is.na(mean)]$median, rads::round2(synthetic_1$final$values[!is.na(median)]$median, 0))
    
    # rounding to 3 digits
    expect_equal(synthetic_2$final$values[!is.na(mean)]$mean, rads::round2(synthetic_2$final$values[!is.na(mean)]$mean, 3))
    expect_equal(synthetic_2$final$values[!is.na(mean)]$median, rads::round2(synthetic_2$final$values[!is.na(median)]$median, 3))      
  })
  
  test_that('Ensure `abs_threshold` changes the reporting threshhold for absolute changes in proportions', {
    # expect test2 to have more absolute changes because we lowered the the threshold, so more changes should be recorded
    expect_gte(
      length(abs(as.numeric(gsub('%', '', synthetic_2$final$missing[!is.na(abs_change)]$abs_change)))), 
      length(abs(as.numeric(gsub('%', '', synthetic_1$final$missing[!is.na(abs_change)]$abs_change))))
    )      
  })

  test_that('Ensure `rel_threshold` changes the reporting threshhold for absolute changes in means & medians', {
    # expect test2 to have more relative changes because we lowered the the threshold, so more changes should be recorded
    expect_gte(
      length(abs(as.numeric(gsub('%', '', synthetic_2$final$values[!is.na(rel_mean_change )]$rel_mean_change )))), 
      length(abs(as.numeric(gsub('%', '', synthetic_1$final$values[!is.na(rel_mean_change )]$rel_mean_change )))))
    
    expect_gte(
      length(abs(as.numeric(gsub('%', '', synthetic_2$final$values[!is.na(rel_median_change )]$rel_median_change )))), 
      length(abs(as.numeric(gsub('%', '', synthetic_1$final$values[!is.na(rel_median_change )]$rel_median_change )))))
  })

# test etl_qa_run_pipeline error messages ----
  test_that("Need to submit a valid data_source_type", {
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'mydata_source',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols =c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "data_source_type must be one of 'r_dataframe', 'sql_server', or 'rads'"
    )})
  
  test_that("Need a valid output directory", {
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols =c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = 'X:/randomDir/randomDir2'
      ), 
      "You have specified an output_directory that does not exist."
    )})
    
  test_that("Need to submit a valid DBI connection", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = 'myconnection',
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols =c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "For 'sql_server' data_source_type, connection must be a DBIConnection object"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = NULL,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols =c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "For 'sql_server' data_source_type, a DBIConnection object must be provided for the connection argument"
    )
    

    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myNEWconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols =c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "It may have been disconnected"
    )
    
    })
  
  test_that("Need data_params to be list", {
    expect_error(
    test <- etl_qa_run_pipeline(
      data_source_type = 'sql_server',
      connection = myconnection,
      data_params = c(
        schema_table = 'birth.final_analytic',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('chi_age'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    ), 
    "data_params must be a list"
  )
  })
  
  test_that("Need data_params$check_chi to be a logical", {
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c('chi_age'), 
          check_chi = 'FALSE'
        ), 
        output_directory = myOutputFolder
      ), 
      "must be a logical "
    )
  })
  
  test_that("Specification of data_params$cols", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c(''), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "No valid 'cols' have been selected"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          # cols = c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "must specify the 'data_params\\$cols' argument"
    )
  })
  
  test_that("Specification of data_params$time_range", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2022),
          cols = c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "`data_params\\$time_range` must be provided and must be a numeric or integer vector of length 2."
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          # time_range = c(2021, 2022),
          cols = c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "`data_params\\$time_range` must be provided and must be a numeric or integer vector of length 2."
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2022, 2021),
          cols = c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "The first value of `data_params\\$time_range` must be less than or equal to the second value."
    )
  })

  test_that("Specification of data_params$time_var", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = 'myyear',
          time_range = c(2021, 2022),
          cols = c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "is not available in this dataset"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          time_var = c('chi_year', 'date_of_birth_year'),
          time_range = c(2021, 2022),
          cols = c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "`data_params\\$time_var` must be the name of a single time variable"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_analytic',
          # time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "`data_params\\$time_var` is missing and must be provided."
    )
  })
  
  test_that("Specification of data_params$data", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          # data = birth_data,
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city', 
                   'num_prev_cesarean', 'mother_date_of_birth'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "For 'r_dataframe' type, data_params must include a 'data' element that is a data.frame or data.table"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = birth_datax,
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city', 
                   'num_prev_cesarean', 'mother_date_of_birth'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "object 'birth_datax' not found"
    )    
    
  })  
  
  test_that("Specification of data_params$function_name", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'rads',
        data_params = list(
          function_name = 'get_data_blahblah',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c('chi_age'),
          version = 'final', 
          kingco = FALSE, 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "The data_params\\$function_name you provided is invalid."
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'rads',
        data_params = list(
          # function_name = 'get_data_birth',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c('chi_age'),
          version = 'final', 
          kingco = FALSE, 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "For 'rads' data_source_type, data_params must include a 'function_name'."
    )    
    
  })  
  
  test_that("Specification of data_params$kingco", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'rads',
        data_params = list(
          function_name = 'get_data_birth',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c('chi_age'),
          version = 'final', 
          kingco = 'FALSE', 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "If provided, `data_params\\$kingco` must be a single logical value"
    )
    
    expect_message(
      test <- etl_qa_run_pipeline(
        data_source_type = 'rads',
        data_params = list(
          function_name = 'get_data_birth',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c('chi_age'),
          version = 'final', 
          #kingco = FALSE, 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "Defaulting to TRUE"
    )    
    
  })  
  
  test_that("Specification of data_params$version", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'rads',
        data_params = list(
          function_name = 'get_data_birth',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c('chi_age'),
          version = 'blah', 
          kingco = FALSE, 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "Invalid value for data_params\\$version. It must be either 'stage' or 'final'."
    )
    
    expect_message(
      test <- etl_qa_run_pipeline(
        data_source_type = 'rads',
        data_params = list(
          function_name = 'get_data_birth',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols = c('chi_age'),
          # version = 'final', 
          kingco = FALSE, 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "Defaulting to 'stage'"
    )    
    
  })  
  
  test_that("Specification of data_params$schema_table", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          # schema_table = 'birth.final_analytic',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols =c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "For 'sql_server' data_source_type, data_params\\$schema_table must be provided in the format 'schema.table'"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth.final_BLAH',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols =c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "The table specified by the data_params\\$schema_table argument does not exist."
    )  
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'sql_server',
        connection = myconnection,
        data_params = list(
          schema_table = 'birth_final_analytic',
          time_var = 'chi_year',
          time_range = c(2021, 2022),
          cols =c('chi_age'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder
      ), 
      "with a period between the schema and table"
    )    
    
  })  
  
  test_that("Specification of digits_mean argument", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = NULL, 
        digits_prop = 3, 
        abs_threshold = 3, 
        rel_threshold = 2
      ), 
      "digits_mean must be a non-negative integer"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = -1, 
        digits_prop = 3, 
        abs_threshold = 3, 
        rel_threshold = 2
      ), 
      "digits_mean must be a non-negative integer"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = "0", 
        digits_prop = 3, 
        abs_threshold = 3, 
        rel_threshold = 2
      ), 
      "digits_mean must be a non-negative integer"
    )
  })  
  
  test_that("Specification of digits_prop argument", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = NULL, 
        abs_threshold = 3, 
        rel_threshold = 2
      ), 
      "digits_prop must be a non-negative integer"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = NULL, 
        abs_threshold = -3, 
        rel_threshold = 2
      ), 
      "digits_prop must be a non-negative integer"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = '3', 
        abs_threshold = 3, 
        rel_threshold = 2
      ), 
      "digits_prop must be a non-negative integer"
    )
  })
    
  test_that("Specification of abs_threshold argument", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = 3, 
        abs_threshold = NULL, 
        rel_threshold = 2
      ), 
      "abs_threshold must be a non-negative number \\[0, 100\\]"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = 3, 
        abs_threshold = -2, 
        rel_threshold = 2
      ), 
      "abs_threshold must be a non-negative number \\[0, 100\\]"
    )

    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = 3, 
        abs_threshold = '2', 
        rel_threshold = 2
      ), 
      "abs_threshold must be a non-negative number \\[0, 100\\]"
    )

    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = 3, 
        abs_threshold = 120, 
        rel_threshold = 2
      ), 
      "abs_threshold must be a non-negative number \\[0, 100\\]"
    )
  })
  
  test_that("Specification of rel_threshold argument", {
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = 3, 
        abs_threshold = 3, 
        rel_threshold = NULL
      ), 
      "rel_threshold must be a non-negative number \\[0, 100\\]"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = 3, 
        abs_threshold = 3, 
        rel_threshold = -2
      ), 
      "rel_threshold must be a non-negative number \\[0, 100\\]"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = 3, 
        abs_threshold = 3, 
        rel_threshold = '2'
      ), 
      "rel_threshold must be a non-negative number \\[0, 100\\]"
    )
    
    expect_error(
      test <- etl_qa_run_pipeline(
        data_source_type = 'r_dataframe',
        data_params = list(
          data = synthetic_data,
          time_var = 'myyear',
          time_range = c(2011, 2020),
          cols = c('mycategorical', 'myinteger'), 
          check_chi = FALSE
        ), 
        output_directory = myOutputFolder, 
        digits_mean = 0, 
        digits_prop = 3, 
        abs_threshold = 3, 
        rel_threshold = 120
      ), 
      "rel_threshold must be a non-negative number \\[0, 100\\]"
    )
    
  })  
  
# test `default_value` (`%||%`) ----
  expect_equal(10L %||% 100, 10L) 
  expect_equal(NULL %||% 100, 100) 
  