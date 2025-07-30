#' Set or Update a Keyring
#'
#' @description
#' This function creates or updates a keyring with a specified username.
#'
#' @note
#' This function replaces the deprecated `apde_keyring_set_f` function from the
#' `apde` package.
#'
#' @param keyring A character string specifying the name of the keyring. If not provided, a dialog will prompt for input.
#' @return A character message indicating the keyring and username that have been set.
#' @examples
#' \dontrun{
#' apde_keyring_set("my_keyring")
#' }
#'
#' @export
#'
apde_keyring_set <- function(keyring = NA){
  username <- NA
  if(is.na(keyring)) {
    keyring <- svDialogs::dlgInput("Keyring:")$res
  }
  username <- svDialogs::dlgInput("Username:")$res
  if(length(keyring) == 0 || is.na(keyring)) {
    stop("Missing Keyring!")
  } else if(length(username) == 0 || is.na(username)) {
    stop("Missing Username!")
  }
  keyring::key_set(service = keyring,
                   username = username)
  message(paste0("Keyring \"", keyring, "\" has been set for username \"", username, "\""))
}

#' Check and Create Keyring if Needed
#'
#' @description
#' This function checks if a keyring exists and creates it if it does not.
#'
#' @note
#' This function replaces the deprecated `apde_keyring_check_f` function from the
#' `apde` package.
#'
#' @param keyring A character string specifying the name of the keyring.
#' @return A character message indicating whether the keyring exists or has been created.
#' @examples
#' \dontrun{
#' apde_keyring_check("my_keyring")
#' }
#'
#' @export
#'
apde_keyring_check <- function(keyring){
  if(nrow(keyring::key_list(keyring)) == 0) {
    message(paste0("Keyring \"", keyring, "\" does not exist."))
    apde_keyring_set(keyring)
  } else {
    message(paste0("Keyring \"", keyring, "\" exists."))
  }
}
