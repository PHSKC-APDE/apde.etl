test_that("tsql_convert_types errors when ph.data missing", {
  expect_error(tsql_convert_types(NULL, field_types = c(a = "int")))
})

test_that("tsql_convert_types errors when field_types missing or unnamed", {
  dt <- data.table(a = 1)
  expect_error(tsql_convert_types(dt, field_types = NULL))
  expect_error(tsql_convert_types(dt, field_types = c("int")))
})

test_that("tsql_convert_types renames columns to match field_types casing", {
  dt <- data.table(A = 1, b = 2)

  ft <- c(a = "int", B = "int")

  out <- tsql_convert_types(dt, ft)

  expect_equal(names(out), c("a", "B"))
})

test_that("tsql_convert_types converts safe numeric strings to integers", {
  dt <- data.table(a = c("1", "2", "3"))
  ft <- c(a = "int")

  out <- tsql_convert_types(dt, ft)

  expect_type(out$a, "integer")
})

test_that("tsql_convert_types avoids unsafe conversions", {
  dt <- data.table(a = c("1", "2", "x"))
  ft <- c(a = "int")

  result <- tsql_convert_types(dt, ft, return_log = TRUE)
  log <- result$conversion_log

  expect_false(log$conversion_success)
  expect_equal(result$data$a, dt$a)
})

test_that("tsql_convert_types returns conversion log when requested", {
  dt <- data.table(a = c("1", "2", "3"))
  ft <- c(a = "int")

  result <- tsql_convert_types(dt, ft, return_log = TRUE)

  expect_true("conversion_log" %in% names(result))
  expect_equal(nrow(result$conversion_log), 1)
})
