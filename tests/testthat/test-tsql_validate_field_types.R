test_that("tsql_validate_field_types errors when ph.data missing", {
  expect_error(tsql_validate_field_types(NULL, field_types = c(a = "int")))
})

test_that("tsql_validate_field_types errors when field_types missing or unnamed", {
  dt <- data.table(a = 1)
  expect_error(tsql_validate_field_types(dt, NULL))
  expect_error(tsql_validate_field_types(dt, c("int")))
})

test_that("tsql_validate_field_types passes valid simple case", {
  dt <- data.table(
    a = 1:10,
    b = c("x", "y")
  )

  ft <- c(
    a = "int",
    b = "nvarchar(10)"
  )

  expect_message(tsql_validate_field_types(dt, ft))
})

test_that("tsql_validate_field_types errors on incompatible type", {
  dt <- data.table(a = c(TRUE, FALSE))
  ft <- c(a = "int")  # logical cannot safely map to int

  expect_error(tsql_validate_field_types(dt, ft))
})

test_that("tsql_validate_field_types warns on nonstandard but allowed conversion", {
  dt <- data.table(a = 1:3)
  ft <- c(a = "nvarchar(10)")

  expect_warning(tsql_validate_field_types(dt, ft))
})

test_that("tsql_validate_field_types detects integer overflow", {
  dt <- data.table(a = 300L)
  ft <- c(a = "tinyint")

  expect_error(tsql_validate_field_types(dt, ft))
})

test_that("tsql_validate_field_types handles all-NA columns correctly", {
  dt <- data.table(a = c(NA, NA))
  ft <- c(a = "nvarchar(10)")

  expect_error(suppressWarnings(tsql_validate_field_types(dt, ft)))

  ft <- c(a = "bit")
  expect_warning(tsql_validate_field_types(dt, ft), '100% missing')
})

