#' Create Database Connection Function
#'
#' @author Jeremy Whitehurst
#' @author Kai Fukutaki
#'
#' @description
#' `r lifecycle::badge("deprecated")`
#'
#' `create_db_connection()` was deprecated in apde.etl 2.5.0. Please use
#' [apde.connect::connect()] instead.
#'
#' Create a connection to APDE prod or dev servers. While deprecated, this
#' function continues to work, but it uses [apde.connect::connect()] under the
#' hood. It always tries Windows integrated authentication first and then falls
#' back to the credentials stored in the `'hhsaw'` `keyring::` service. The
#' `interactive` argument is ignored.
#'
#' @note
#' This function replaces the deprecated `create_db_connection` function from the
#' `apde` package.
#'
#' @param server Whether we are connecting to `'hhsaw'`, `'inthealth'`,
#' or `'phextractstore'`. Note that `'inthealth'` connects to the
#' `inthealth_edw` database.
#' @param prod Whether the production or dev/WIP servers are to be used. Defaults
#' to `prod = TRUE`.
#' @param interactive **Ignored.** Kept only so that existing code does not
#' break. `interactive = TRUE` used to request `ActiveDirectoryInteractive`
#' (pop-up) authentication, which is no longer supported. Now, regardless of
#' this value, the function uses Windows integrated authentication followed by
#' `keyring::` credentials (i.e., `apde.connect::connect(auth = 'auto')`).
#' Defaults to `interactive = FALSE`.
#'
#' @details
#' **Switching to `apde.connect::connect()`:**
#'
#' | **Old code**                                                  | **New code**                                                      |
#' |---------------------------------------------------------------|-------------------------------------------------------------------|
#' | `create_db_connection(server = 'hhsaw')`                      | `apde.connect::connect(database = 'hhsaw')`                        |
#' | `create_db_connection(server = 'inthealth')`                  | `apde.connect::connect(database = 'inthealth_edw')`                |
#' | `create_db_connection(server = 'phextractstore')`             | `apde.connect::connect(database = 'phextractstore')`               |
#' | `create_db_connection(server = 'hhsaw', prod = FALSE)`        | `apde.connect::connect(database = 'hhsaw', prod = FALSE)`          |
#'
#' @return A database connection object
#'
#' @examples
#' \dontrun{
#' # Deprecated. Please use apde.connect::connect() instead.
#'
#' # Connect to production HHSAW
#' conn <- create_db_connection(server = "hhsaw", prod = TRUE)
#'
#' # Connect to development inthealth server
#' conn <- create_db_connection(server = "inthealth", prod = FALSE)
#' }
#'
#' @export
#'
create_db_connection <- function(server = c("phextractstore", "hhsaw", "inthealth"),
                                 prod = TRUE,
                                 interactive = FALSE) {

  lifecycle::deprecate_warn(
    when = "2.5.0",
    what = "create_db_connection()",
    with = "apde.connect::connect()",
    details = "create_db_connection() still works but now calls apde.connect::connect() under the hood. See ?create_db_connection for how to update your code."
  )

  # validate arguments ----
  server <- match.arg(server)

  if (!is.logical(prod) || length(prod) != 1 || is.na(prod)) {
    stop("'prod' must be a logical value (TRUE/FALSE)")
  }

  # `interactive` is intentionally ignored (see @param interactive)

  # connect via apde.connect ----
  database <- switch(server,
                     hhsaw = "hhsaw",
                     inthealth = "inthealth_edw",
                     phextractstore = "phextractstore")

  # 'auto' = Windows integrated authentication, then keyring fallback
  apde.connect::connect(database = database, prod = prod, auth = "auto")
}
