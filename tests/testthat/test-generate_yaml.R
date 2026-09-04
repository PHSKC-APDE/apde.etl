test_that("generate_yaml errors with missing mydt", {
  expect_error(generate_yaml(NULL, schema = "sch", table = "tbl"))
})

test_that("generate_yaml errors when mydt is not data.frame/data.table", {
  expect_error(generate_yaml("not a df", schema = "sch", table = "tbl"))
})

test_that("generate_yaml warns and returns list when no outfile is given", {
  dt <- data.table(a = 1, b = "x")

  expect_message({
    y <- generate_yaml(
      mydt = dt,
      schema = "sch",
      table = "tbl",
      datasource = "source"
    )
  })

  expect_type(y, "list")
  expect_true("vars" %in% names(y))
})

test_that("generate_yaml correctly writes YAML file when outfile is provided", {
  dt <- data.table(a = 1:3, b = c("x", "y", "z"))
  outfile <- tempfile(fileext = ".yaml")

  expect_message(
    generate_yaml(
      mydt = dt,
      outfile = outfile,
      schema = "sch",
      table = "tbl",
      datasource = "source"
    )
  )

  y <- yaml::read_yaml(outfile)

  expect_equal(y$schema, "sch")
  expect_equal(y$table, "tbl")
  expect_equal(names(y$vars), c("a", "b"))
})

test_that("generate_yaml detects binary variables", {

  # these are numeric, but not explicit integers so will get 'INT'
  dt <- data.table(x = c(0, 1, NA))

  y <- generate_yaml(
    mydt = dt,
    schema = "sch",
    table = "tbl",
    datasource = "source"
  )

  expect_true(grepl("INT", y$vars$x))

  # explicit integers will give 'BIT'
  dt <- data.table(x = c(0L, 1L, NA))

  y <- generate_yaml(
    mydt = dt,
    schema = "sch",
    table = "tbl",
    datasource = "source"
  )

  expect_true(grepl("BIT", y$vars$x))
})
