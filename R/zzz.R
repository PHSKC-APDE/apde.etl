.onAttach <- function(libname, pkgname) {
  if (interactive()) {
    check_version()
  }
}

.onLoad <- function(libname, pkgname) {

  # Find the option to use
  types <- unique(odbc::odbcListDrivers()$name)

  # Find the best ODBC driver, if available
  odbc_drv <- grep('ODBC Driver', types, value = TRUE)
  odbc_drv <- sort(odbc_drv)

  if(length(odbc_drv) > 0) {
    ov <- odbc_drv[length(odbc_drv)]
  } else {
    ov <- "SQL Server"
    warning('No usable ODBC driver detected. To use functions requiring a database connection please install an ODBC driver. "SQL Server" is being used as default which may not work super well. Good luck.')
  }

  options(
    apde.etl.odbc_version = ov
  )
}
