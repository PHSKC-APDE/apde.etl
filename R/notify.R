#---- HIGH-LEVEL (User-facing) FUNCTIONS ####
# apde_notify() ----
#' Send APDE Notification
#'
#' @description
#' Sends an email notification to a predefined list of recipients using
#' a stored message template. The function retrieves the message content and
#' recipient list from the database, performs variable substitution in the
#' message body and subject line, and sends the email via Outlook SMTP. If email
#' credentials are not set up, it will prompt the user to configure them.
#'
#' @details
#' This function handles the complete email sending workflow: validates message
#' exists, retrieves recipient list, substitutes template variables, and sends
#' via SMTP. Message templates and recipient lists must be pre-configured using
#' [apde_notify_menu()].
#'
#' @note
#' This function replaces the deprecated `apde_notify_f()` function from the
#' `apde` package.
#'
#' @param msg_id Integer. Database ID of the message template to send. Either
#' `msg_id` or `msg_name` must be provided.
#' @param msg_name Character. Name of the message template to send. Either
#' `msg_id` or `msg_name` must be provided.
#' @param vars Named list. Variables to substitute in the message body and
#' subject line.
#'
#' @seealso
#' [apde_notify_set_cred()] which is called upon to set the credentials
#'
#' @return None
#'
#' @export
#'
#' @examples
#'  \dontrun{
#'   # Send a notification using message name
#'   apde_notify(msg_name = "daily_report",
#'                 vars = list(date = Sys.Date()))
#'
#'   # Send using message name
#'   apde_notify(msg_name = 'claims_mcaid_update',
#'                 vars = list(count = 42, status = "complete"))
#'
#'   # Send using message ID
#'   apde_notify(msg_id = 1,
#'                 vars = list(count = 42, status = "complete"))
#'  }
#'
apde_notify <- function(msg_id = NULL,
                          msg_name = NULL,
                          vars) {
  emailReady <- tryCatch(
    { length(blastula::creds_key("outlook")) },
    error = function(x) { return(0) })

  if(emailReady == 0) {
    apde_notify_set_cred()
  }

  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  if(is.null(msg_id)) {
    msg_id <- apde_notify_msg_id_get(msg_name = msg_name)
  }
  if(is.null(msg_id)) {
    stop("No Message in the system with the provided Name or ID.")
  }
  msg <- apde_notify_msg_get(msg_id = msg_id)
  email_list <- apde_notify_list_get(msg_id = msg_id)
  vars <- as.list(vars)
  email <- blastula::compose_email(
    body = blastula::md(glue::glue(msg$msg_body)))
  blastula::smtp_send(
    email = email,
    to = email_list$address,
    from = msg$msg_from,
    subject = glue::glue(msg$msg_subject),
    credentials = blastula::creds_key("outlook")
  )
}

# apde_notify_menu() ----
#' Launch APDE Notify Management Interface
#'
#' @description
#' Launches an interactive Shiny web application for creating, editing, and
#' managing email notification templates and recipient lists. This is the primary
#' interface for setting up the notification system before using [apde_notify()]
#' to send emails. The app provides forms to create message templates with variable
#' placeholders, manage email addresses, and assign recipients to specific messages.
#'
#' @details
#' The Shiny app includes three main sections:
#'
#' - **Message Management**: Create/edit email templates with subject, body,
#'   and sender address. Supports glue syntax for variable substitution.
#' - **Recipient Lists**: Assign email addresses to specific message templates
#'   using a multi-select interface.
#' - **Address Management**: Add, edit, or remove email addresses from the
#'   system-wide address book.
#'
#' All changes are saved directly to the HHSAW and are immediately available for
#' use with [apde_notify()].
#'
#' @note
#' This function replaces the deprecated `apde_notify_menu_f()` function from the
#' `apde` package.
#'
#' @seealso
#' [apde_notify()] for sending notifications using templates created here
#'
#' @return A Shiny app object that launches the management interface
#'
#' @export
#'
#' @examples
#'  \dontrun{
#'   # Launch the notification management Shiny app
#'   apde_notify_menu()
#'
#'   # Typical workflow:
#'   # 1. Run apde_notify_menu()
#'   # 2. Add email addresses to the system
#'   # 3. Create message templates
#'   # 4. Assign recipients to each message
#'   # 5. Use apde_notify() to send notifications
#'  }
#'
apde_notify_menu <- function() {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_list <- apde_notify_addresses_get()
  current_list <- c()

  ui <- shiny::fluidPage(
    shiny::titlePanel("APDE Notify Menu"),
    shiny::fluidRow(
      shiny::column(4,
                    shiny::selectInput(inputId = "msg_select", label = "Select Message",
                                       choices = c("New Message", as.list(apde_notify_msgs_get()$msg_name))),
                    shiny::textInput(inputId = "msg_name_text", label = "Message Name"),
                    shiny::textInput(inputId = "msg_subject_text", label = "Message Subject"),
                    shiny::textInput(inputId = "msg_from_text", label = "Message From Address"),
                    shiny::textAreaInput(inputId = "msg_body_textarea", label = "Message Body",
                                         height = 200),
                    shiny::actionButton(inputId = "msg_save_btn", label = "Save Message")
      ),
      shiny::column(8,
                    shinyWidgets::multiInput(inputId = "multi_list", label = "Email List",
                                             width = 600, choices = as.list(address_list$address)),
                    shiny::actionButton(inputId = "list_save_btn", label = "Save Email List"),
                    shiny::hr(),
                    shiny::selectInput(inputId = "address_select", label = "Select Email Address",
                                       choices = c("New Email Address", as.list(address_list$address))),
                    shiny::textInput(inputId = "address_text", label = "Email Address"),
                    shiny::actionButton(inputId = "address_save_btn", label = "Save Email Address")
      )
    ),
    shiny::fluidRow(
      shiny::column(10, offset = 1,
                    shiny::hr(),
                    shiny::textOutput("output_msg"),
                    shiny::hr(),
      )
    )
  )

  server <- function(input, output, session) {
    text_reactive <- shiny::reactiveValues(text = "")

    shiny::observeEvent(input$msg_select, {
      if(input$msg_select != "New Message") {
        msg_id <- apde_notify_msg_id_get(msg_name = input$msg_select)
        current_list <- apde_notify_list_get(msg_id = msg_id)
        msg <- apde_notify_msg_get(msg_id = msg_id)
        shiny::updateTextInput(session = session,
                               inputId = "msg_name_text",
                               value = msg$msg_name)
        shiny::updateTextInput(session = session,
                               inputId = "msg_subject_text",
                               value = msg$msg_subject)
        shiny::updateTextInput(session = session,
                               inputId = "msg_from_text",
                               value = msg$msg_from)
        shiny::updateTextAreaInput(session = session,
                                   inputId = "msg_body_textarea",
                                   value = msg$msg_body)
      } else {
        current_list = c()
        shiny::updateTextInput(session = session,
                               inputId = "msg_name_text",
                               value = NA)
        shiny::updateTextInput(session = session,
                               inputId = "msg_subject_text",
                               value = NA)
        shiny::updateTextInput(session = session,
                               inputId = "msg_from_text",
                               value = NA)
        shiny::updateTextAreaInput(session = session,
                                   inputId = "msg_body_textarea",
                                   value = NA)

      }

      address_list <- apde_notify_addresses_get()
      shinyWidgets::updateMultiInput(session = session,
                                     inputId = "multi_list",
                                     choices = as.list(address_list$address),
                                     selected = as.list(current_list$address))
    })

    shiny::observeEvent(input$msg_save_btn, {
      if(input$msg_select == "New Message") {
        msg_id <- 0
      } else {
        msg_id <- apde_notify_msg_id_get(msg_name = input$msg_select)
      }
      mname <- input$msg_name_text
      apde_notify_msg_set(msg_id,
                            msg_name = input$msg_name_text,
                            msg_subject = input$msg_subject_text,
                            msg_body = input$msg_body_textarea,
                            msg_from = input$msg_from_text)
      if(input$msg_select == "New Message") {
        text_reactive$text <- paste0("Message: '", input$msg_name_text, "' created - ", Sys.time())
      } else {
        text_reactive$text <- paste0("Message: '", input$msg_name_text, "' updated - ", Sys.time())
      }
      shiny::updateSelectInput(session = session,
                               inputId = "msg_select",
                               choices = c("New Message", as.list(apde_notify_msgs_get()$msg_name)),
                               selected = mname)
    })

    shiny::observeEvent(input$list_save_btn, {
      if(input$msg_select != "New Message") {
        msg_id <- apde_notify_msg_id_get(msg_name = input$msg_select)
        apde_notify_list_set(msg_id = msg_id,
                               choices = input$multi_list)
        text_reactive$text <- paste0("Email List: '", input$msg_name_text, "' updated - ", Sys.time())
      } else {
        text_reactive$text <- paste0("Error: Select a Message before updating the Email List - ", Sys.time())
      }
    })

    shiny::observeEvent(input$address_select, {
      if(input$address_select != "New Email Address") {
        shiny::updateTextInput(session = session,
                               inputId = "address_text",
                               value = input$address_select)
      } else {
        shiny::updateTextInput(session = session,
                               inputId = "address_text",
                               value = NA)
      }
    })

    shiny::observeEvent(input$address_save_btn, {
      if(input$address_select != "New Email Address") {
        print(input$address_select)
        address_id <- apde_notify_address_id_get(address = input$address_select)
        apde_notify_address_set(address_id = address_id,
                                  new_address = input$address_text)
        text_reactive$text <- paste0("Email Address: '", input$address_text, "' updated - ", Sys.time())
      } else {
        apde_notify_address_create(address = input$address_text)
        text_reactive$text <- paste0("Email Address: '", input$address_text, "' created - ", Sys.time())
      }
      if(input$msg_select != "New Message") {
        current_list <- apde_notify_list_get(msg_name = input$msg_select)
      } else {
        current_list <- c()
      }
      address <- input$address_text
      address_list <- apde_notify_addresses_get()
      shinyWidgets::updateMultiInput(session = session,
                                     inputId = "multi_list",
                                     choices = as.list(address_list$address),
                                     selected = as.list(current_list$address))
      shiny::updateSelectInput(session = session,
                               inputId = "address_select",
                               choices = c("New Email Address", as.list(address_list$address)),
                               selected = address)
    })

    output$output_msg <- shiny::renderText({ text_reactive$text })
  }

  shiny::shinyApp(ui = ui, server = server)
}




#---- MID-LEVEL FUNCTIONS (messages and list management) ####
# apde_notify_set_cred() ----
#' Set Up Outlook Email Credentials for APDE Notify
#'
#' @description
#' Configures and stores Outlook SMTP credentials required for sending email
#' notifications. This function prompts for your King County email address and
#' password, then securely stores the credentials using the blastula package's
#' key management system. The stored credentials are automatically used by
#' [apde_notify()] when sending emails.
#'
#' @details
#' This function creates an interactive setup process:
#'
#' 1. Prompts for email address (defaults to your system username + @kingcounty.gov)
#' 2. Opens a secure password dialog for entering your email password
#' 3. Stores credentials with SSL encryption for Outlook SMTP
#' 4. Saves credentials with ID "outlook" for automatic retrieval
#'
#' Credentials are stored locally and persist between R sessions. This function
#' is automatically called by [apde_notify()] if no credentials are
#' found, but can also be run manually to update or reset credentials.
#'
#' @note
#' This function requires interactive input and cannot be run in non-interactive
#' environments. Credentials are stored securely using the operating system's
#' credential management system.
#'
#' This function replaces the deprecated `apde_notify_set_cred_f()` function from the
#' `apde` package.
#'
#' @seealso
#' [apde_notify()] which automatically calls this function when needed
#'
#' @return None
#'
#' @export
#'
#' @examples
#'  \dontrun{
#'   # Set up email credentials manually (although usually called by apde_notify())
#'   apde_notify_set_cred()
#'  }
#'
apde_notify_set_cred <- function() {
  ## CREATING THE OUTLOOK CREDENTIAL
  ## ENTER EMAIL ADDRESS
  email <- svDialogs::dlg_input("Enter Email address:", paste0(Sys.info()["user"], "@kingcounty.gov"))$res
  ## ENTER YOUR PW IN POP UP
  blastula::create_smtp_creds_key(
    id = "outlook",
    user = email,
    provider = "outlook",
    overwrite = TRUE,
    use_ssl = TRUE
  )
}

#---- HELPER FUNCTIONS ####
# apde_notify_addresses_get() ----
#' Retrieve All APDE Notify Addresses
#'
#' @description
#' **Internal function**. Retrieves all email addresses from the notify_addresses table.
#'
#' @details
#' This function connects to the HHSAW database and returns all email addresses
#' stored in the apde.notify_addresses table, ordered alphabetically by address.
#'
#' @return A data.frame containing all notify addresses with columns: id, address
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   addresses <- apde_notify_addresses_get()
#'  }
apde_notify_addresses_get <- function() {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  addresses <- DBI::dbGetQuery(conn, "SELECT *
                               FROM [apde].[notify_addresses]
                               ORDER BY [address] ASC")
  return(addresses)
}

# apde_notify_address_get() ----
#' Retrieve a Specific APDE Notify Address
#'
#' @description
#' **Internal function**. Retrieves a specific email address by ID or address value.
#'
#' @param address_id Integer. ID of the address to retrieve.
#' @param address Character. Email address to retrieve.
#'
#' @details
#' Either address_id or address must be provided. The function will use
#' apde_notify_address_id_get() to resolve the ID if only address is provided.
#'
#' @return A data.frame containing the address information.
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   address_info <- apde_notify_address_get(address = "user@kingcounty.gov")
#'  }
apde_notify_address_get <- function(address_id = NULL,
                                      address = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_id <- apde_notify_address_id_get(address_id = address_id,
                                             address = address)
  if(is.null(address_id)) {
    stop("No Address in the system with the provided Address or ID.")
  }
  address <- DBI::dbGetQuery(conn, glue::glue_sql("SELECT TOP (1) *
                                                  FROM [apde].[notify_addresses]
                                                  WHERE [id] = {address_id}",
                                                  .con = conn))
  return(address)
}

# apde_notify_address_set() ----
#' Update an APDE Notify Address
#'
#' @description
#' **Internal function**. Updates an existing email address in the system.
#'
#' @param address_id Integer. ID of the address to update.
#' @param address Character. Current email address.
#' @param new_address Character. New email address.
#'
#' @details
#' Either address_id or address must be provided to identify the record to update.
#' The function checks that the new_address doesn't already exist before updating.
#'
#' @return None
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   apde_notify_address_set(address = "old@kingcounty.gov",
#'                             new_address = "new@kingcounty.gov")
#'  }
apde_notify_address_set <- function(address_id = NULL,
                                      address = NULL,
                                      new_address) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_id <- apde_notify_address_id_get(address_id = address_id,
                                             address = address)
  if(is.null(address_id)) {
    stop("No Address in the system with the provided Address or ID.")
  }
  new_address_check <- apde_notify_address_id_get(address = new_address)
  if(length(new_address_check) > 0) {
    stop("New Address already exists.")
  }
  DBI::dbExecute(conn, glue::glue_sql("UPDATE [apde].[notify_addresses]
                                      SET [address] = {new_address}
                                      WHERE [id] = {address_id}",
                                      .con = conn))
}

# apde_notify_address_create() ----
#' Create a New APDE Notify Address
#'
#' @description
#' **Internal function**. Creates a new email address in the system.
#'
#' @param address Character. Email address to create.
#'
#' @details
#' The function checks that the address doesn't already exist before creating it.
#'
#' @return Integer. ID of the newly created address.
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   new_id <- apde_notify_address_create(address = "user@kingcounty.gov")
#'  }
apde_notify_address_create <- function(address) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_id <- apde_notify_address_id_get(address = address)
  if(length(address_id) > 0) {
    stop("Address already exists.")
  }
  DBI::dbExecute(conn, glue::glue_sql("INSERT INTO [apde].[notify_addresses]
                                      ([address])
                                      VALUES
                                      ({address})",
                                      .con = conn))
  address_id <- apde_notify_address_id_get(address = address)
  return(address_id)
}

# apde_notify_address_delete() ----
#' Delete an APDE Notify Address
#'
#' @description
#' **Internal function**. Deletes an email address and all associated list memberships.
#'
#' @param address_id Integer. ID of the address to delete.
#' @param address Character. Email address to delete.
#'
#' @details
#' Either address_id or address must be provided. This function will also remove
#' the address from all notification lists before deleting the address record.
#'
#' @return None
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   apde_notify_address_delete(address = "user@kingcounty.gov")
#'  }
apde_notify_address_delete <- function(address_id = NULL,
                                         address = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  address_id <- apde_notify_address_id_get(address_id = address_id,
                                             address = address)
  if(is.null(address_id)) {
    stop("No Address in the system with the provided Address or ID.")
  }
  DBI::dbExecute(conn,
                 glue::glue_sql("DELETE FROM [apde].[notify_list]
                                WHERE [address_id] = {address_id}",
                                .con = conn))
  DBI::dbExecute(conn,
                 glue::glue_sql("DELETE FROM [apde].[notify_addresses]
                                WHERE [id] = {address_id}",
                                .con = conn))
}

# apde_notify_address_id_get() ----
#' Get APDE Notify Address ID
#'
#' @description
#' **Internal function**. Resolves an address ID from either an ID or email address.
#'
#' @param address_id Integer. ID of the address.
#' @param address Character. Email address.
#'
#' @details
#' If address_id is provided, validates it exists. If address is provided,
#' looks up the corresponding ID. Returns NULL if not found.
#'
#' @return Integer. ID of the address, or NULL if not found.
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   id <- apde_notify_address_id_get(address = "user@kingcounty.gov")
#'  }
apde_notify_address_id_get <- function(address_id = NULL,
                                         address = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  addresses <- apde_notify_addresses_get()
  if(is.null(address_id)) {
    address_id <- addresses[addresses$address == address, ]$id
  } else {
    address_id <- addresses[addresses$id == address_id, ]$id
    if (length(address_id) == 0) {
      address_id <- NULL
    }
  }
  return(address_id)
}

# apde_notify_list_get() ----
#' Retrieve APDE Notify List for a Message
#'
#' @description
#' **Internal function**. Gets the email address list associated with a specific message.
#'
#' @param msg_id Integer. ID of the message.
#' @param msg_name Character. Name of the message.
#'
#' @details
#' Either msg_id or msg_name must be provided. Returns all email addresses
#' assigned to receive notifications for the specified message.
#'
#' @return A data.frame containing the notify list with columns: msg_id, address_id, address
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   email_list <- apde_notify_list_get(msg_name = "daily_report")
#'  }
apde_notify_list_get <- function(msg_id = NULL,
                                   msg_name = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  msg_id <- apde_notify_msg_id_get(msg_id = msg_id,
                                     msg_name = msg_name)
  if(is.null(msg_id)) {
    stop("No Message in the system with the provided Name or ID.")
  }
  mlist <- DBI::dbGetQuery(conn,
                           glue::glue_sql("SELECT L.[msg_id], L.[address_id], A.[address]
                                          FROM [apde].[notify_list] L
                                          INNER JOIN [apde].[notify_addresses] A ON L.[address_id] = A.[id]
                                          WHERE L.[msg_id] = {msg_id}
                                          ORDER BY A.[address]",
                                          .con = conn))
  return(mlist)
}

# apde_notify_list_set() ----
#' Set APDE Notify List for a Message
#'
#' @description
#' **Internal function**. Replaces the email address list for a specific message.
#'
#' @param msg_id Integer. ID of the message.
#' @param msg_name Character. Name of the message.
#' @param choices Vector. List of email addresses to associate with the message.
#'
#' @details
#' Either msg_id or msg_name must be provided. This function completely replaces
#' the existing recipient list with the provided choices.
#'
#' @return None
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   apde_notify_list_set(msg_name = "daily_report",
#'                          choices = c("user1@kingcounty.gov", "user2@kingcounty.gov"))
#'  }
apde_notify_list_set <- function(msg_id = NULL,
                                   msg_name = NULL,
                                   choices) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  msg_id <- apde_notify_msg_id_get(msg_id = msg_id,
                                     msg_name = msg_name)
  if(is.null(msg_id)) {
    stop("No Message in the system with the provided Name or ID.")
  }
  DBI::dbExecute(conn,
                 glue::glue_sql("DELETE FROM [apde].[notify_list]
                                WHERE [msg_id] = {msg_id}",
                                .con = conn))
  address_list <- apde_notify_addresses_get()
  if(length(choices) > 0) {
    choices <- as.data.frame(choices)
    colnames(choices) <- c("address")
    choices <- dplyr::inner_join(choices, address_list)
    for(i in 1:nrow(choices)) {
      DBI::dbExecute(conn,
                     glue::glue_sql("INSERT INTO [apde].[notify_list]
                                     ([msg_id], [address_id])
                                     VALUES
                                     ({msg_id}, {choices[i,]$id})",
                                    .con = conn))
    }
  }
}

# apde_notify_msgs_get() ----
#' Retrieve APDE Notify Messages
#'
#' @description
#' **Internal function**. Retrieves all parent messages from the notify_msgs table.
#'
#' @details
#' Returns only parent messages (where msg_parent is NULL), ordered alphabetically
#' by message name. These are the active message templates available for use.
#'
#' @return A data.frame containing message information with columns: id, msg_name, msg_subject, msg_body, msg_from, msg_datetime, msg_parent
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   messages <- apde_notify_msgs_get()
#'  }
apde_notify_msgs_get <- function() {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  msgs <- DBI::dbGetQuery(conn,
                          "SELECT *
                          FROM [apde].[notify_msgs]
                          WHERE msg_parent IS NULL
                          ORDER BY [msg_name] ASC")
  return(msgs)
}

# apde_notify_msg_id_get() ----
#' Get APDE Notify Message ID
#'
#' @description
#' **Internal function**. Resolves a message ID from either an ID or message name.
#'
#' @param msg_id Integer. ID of the message.
#' @param msg_name Character. Name of the message.
#'
#' @details
#' If msg_id is provided, validates it exists. If msg_name is provided,
#' looks up the corresponding ID. Returns NULL if not found.
#'
#' @return Integer. ID of the message, or NULL if not found.
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   id <- apde_notify_msg_id_get(msg_name = "daily_report")
#'  }
apde_notify_msg_id_get <- function(msg_id = NULL,
                                     msg_name = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  msgs <- apde_notify_msgs_get()
  if(is.null(msg_id)) {
    msg_id <- msgs[msgs$msg_name == msg_name, ]$id
  } else {
    msg_id <- msgs[msgs$id == msg_id, ]$id
    if (length(msg_id) == 0) {
      msg_id <- NULL
    }
  }
  return(msg_id)
}

# apde_notify_msg_get() ----
#' Retrieve a Specific APDE Notify Message
#'
#' @description
#' **Internal function**. Retrieves a specific message template by ID or name.
#'
#' @param msg_id Integer. ID of the message to retrieve.
#' @param msg_name Character. Name of the message to retrieve.
#'
#' @details
#' Either msg_id or msg_name must be provided. Returns the complete message
#' template including subject, body, and sender information.
#'
#' @seealso
#' [apde_notify()] & [apde_notify_menu()] which call this function
#'
#' [apde_notify_msg_id_get()] which is called by this function
#'
#' @return A data.frame containing the message information.
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   message <- apde_notify_msg_get(msg_name = "daily_report")
#'  }
apde_notify_msg_get <- function(msg_id = NULL,
                                  msg_name = NULL) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  if(is.null(msg_id)) {
    msg_id <- apde_notify_msg_id_get(msg_name = msg_name)
  }
  if(is.null(msg_id)) {
    stop("No Message in the system with the provided Name or ID.")
  }
  msg <- DBI::dbGetQuery(conn,
                         glue::glue_sql("SELECT TOP (1) *
                                        FROM [apde].[notify_msgs]
                                        WHERE [id] = {msg_id}
                                          AND [msg_parent] IS NULL",
                                        .con = conn))
  return(msg)
}

# apde_notify_msg_set() ----
#' Set or Update APDE Notify Message
#'
#' @description
#' **Internal function**. Creates a new message template or updates an existing one.
#'
#' @param msg_id Integer. ID of the message to update. Use 0 for new messages.
#' @param msg_name Character. Name of the message.
#' @param msg_subject Character. Subject of the message.
#' @param msg_body Character. Body of the message.
#' @param msg_from Character. Sender's email address.
#'
#' @details
#' If msg_id is 0, creates a new message. If msg_id > 0, creates a new version
#' and archives the old one by setting its msg_parent field. This maintains
#' a history of message changes.
#'
#' @seealso
#' [apde_notify_menu()] which calls this function
#'
#' [apde_notify_msg_get()] which is called by this function
#'
#' @return Integer. ID of the new or updated message.
#'
#' @keywords internal
#'
#' @examples
#'  \dontrun{
#'   new_id <- apde_notify_msg_set(msg_id = 0,
#'                                   msg_name = "test_message",
#'                                   msg_subject = "Test Subject",
#'                                   msg_body = "Test body",
#'                                   msg_from = "sender@kingcounty.gov")
#'  }
apde_notify_msg_set <- function(msg_id = 0,
                                  msg_name,
                                  msg_subject,
                                  msg_body,
                                  msg_from) {
  conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  if(msg_id > 0) {
    msg <- apde_notify_msg_get(msg_id)
    if(msg$msg_name == msg_name & msg$msg_subject == msg_subject &
       msg$msg_body == msg_body & msg$msg_from == msg_from) {
      return(msg_id)
    }
  }
  DBI::dbExecute(conn,
                 glue::glue_sql("INSERT INTO [apde].[notify_msgs]
                                ([msg_name], [msg_subject], [msg_body],
                                [msg_from], [msg_datetime])
                                VALUES
                                ({msg_name}, {msg_subject}, {msg_body},
                                {msg_from}, GETDATE())",
                                .con = conn))
  new_id <- DBI::dbGetQuery(conn,
                            glue::glue_sql("SELECT TOP (1) id
                                           FROM [apde].[notify_msgs]
                                           WHERE [msg_name] = {msg_name}
                                            AND msg_parent IS NULL
                                           ORDER BY msg_datetime DESC",
                                           .con = conn))[1]
  if(msg_id > 0) {
    DBI::dbExecute(conn,
                   glue::glue_sql("UPDATE [apde].[notify_msgs]
                                  SET [msg_parent] = {new_id}
                                  WHERE id = {msg_id}",
                                  .con = conn))
    DBI::dbExecute(conn,
                   glue::glue_sql("UPDATE [apde].[notify_msgs]
                                  SET [msg_parent] = {new_id}
                                  WHERE [msg_parent] = {msg_id}",
                                  .con = conn))
    DBI::dbExecute(conn,
                   glue::glue_sql("UPDATE [apde].[notify_list]
                                  SET [msg_id] = {new_id}
                                  WHERE [msg_id] = {msg_id}",
                                  .con = conn))
  }
  return(new_id)
}
