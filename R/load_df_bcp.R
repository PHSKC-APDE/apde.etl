#' Load an R Data.Frame to SQL Server Using BCP (Bulk Copy Program)
#'
#' @description This function loads data into a SQL Server database table using
#' the [BCP (Bulk Copy Program)](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?)
#' utility. Data.frames are written to a temporary file before loading; file
#' paths are used directly.
#'
#' @author Jeremy Whitehurst, 2025-02-05
#'
#' @note
#' This function replaces the deprecated `load_df_bcp_f` function from the
#' `apde` package.
#'
#' @param dataset A data.frame or data.table to be loaded, or a character string
#' specifying the filepath of a text file to be loaded.
#' @param server For on-premises servers: the SQL Server instance name (e.g.,
#' 'SQLSERVER01'). For Azure SQL servers: the name of an ODBC System DSN that
#' you must create first (see Details section for setup instructions).
#'
#' *Note!* `server = 'PHClaims'` will be mapped to `'KCITSQLPRPENT40'`
#' @param db_name The name of the target database.
#' @param schema_name The name of the schema containing the target table.
#' @param table_name The name of the target table.
#' @param user Optional. The username for SQL Server authentication. If provided,
#' `pass` must also be specified.
#' @param pass Optional. The password for SQL Server authentication. Required if
#' `user` is provided.
#'
#' @details
#' You will need to install the [BCP utility](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?)
#' to efficiently load data into a SQL Server table. Use the "Download
#' Microsoft Command Line Utilities 15 for SQL Server (x64)" version. To check
#' if BCP is already installed, just open your Windows command prompt and type
#' `bcp`. If it is installed, you'll see BCP usage information and syntax. If it
#' isn't installed, you'll see an error like 'bcp is not recognized as an internal
#' or external command'. After installation, it will support both Windows
#' authentication and SQL Server authentication.
#'
#' **TO USE WITH AZURE:** You must create an ODBC Data Source Name (DSN) that
#' matches your server name. Open "ODBC Data Sources (64-bit)" from Windows Start
#' menu, go to System DSN tab, click Add, and select "ODBC Driver 17 for SQL Server"
#' (NOT Driver 18, which has compatibility issues with BCP). Configure the DSN with
#' your server name as the Name field, select "With Azure Active Directory Password
#' authentication", enter your login credentials, and test the connection. The DSN
#' name must exactly match the server parameter you pass to this function.
#'
#' The function uses the following pre-specified BCP parameters:
#' - **`-r \\n`**: Row terminator (newline character)
#' - **`-t \\t`**: Field terminator (tab character)
#' - **`-C 65001`**: Use UTF-8 encoding
#' - **`-F 2`**: Skip the first row (assumes headers)
#' - **`-b 100000`**: Batch size of 100,000 rows
#' - **`-c`**: Character data type format
#' - **`-q`**: Use quoted identifiers for special characters in names
#'
#' If the BCP command fails, the function will stop with an error message for
#' troubleshooting.
#'
#' @seealso [tsql_chunk_loader][rads::tsql_chunk_loader] for a pure R alternative with
#' retry logic, data type validation, and finer control. It is better suited for
#' moderate-sized datasets.
#'
#' @return A character vector containing the output of the BCP command.
#'
#' @export
#'
#' @examples
#'  \dontrun{
#'   # Note: Target table must exist before loading data!
#'
#'   # Create example data
#'   df <- data.frame(id = 1:5, name = c("Anne", "Blishda", "Carrie", "Daniel", "Eva"))
#'
#'   # Load data to on-premises server using Windows authentication
#'   load_df_bcp(dataset = df,
#'               server = "KCITSQLUATHIP40",# actual server name
#'               db_name = "phextractstore",
#'               schema_name = "APDE_WIP",
#'               table_name = "my_table")
#'
#'   # Load data to Azure server using ODBC DSN and stored credentials
#'   load_df_bcp(dataset = df,
#'               server = "hhsaw_dev", # ODBC DSN
#'               db_name = "hhs_analytics_workspace",
#'               schema_name = "dbo",
#'               table_name = "my_table",
#'               user = keyring::key_list("hhsaw_dev")[["username"]],
#'               pass = keyring::key_get("hhsaw_dev", keyring::key_list("hhsaw_dev")[["username"]]))
#'
#'   # Load data from a file path directly
#'   load_df_bcp(dataset = "C:/path/to/data.txt",
#'               server = "hhsaw_dev",
#'               db_name = "hhs_analytics_workspace",
#'               schema_name = "dbo",
#'               table_name = "my_table",
#'               user = "myuser",
#'               pass = "mypassword")
#'  }
#'
load_df_bcp <- function(dataset,
                       server,
                       db_name,
                       schema_name,
                       table_name,
                       user = NULL,
                       pass = NULL
) {

  if(is.character(dataset)){
    filepath = dataset
  }else{
    filepath = tempfile(fileext = '.txt')
    on.exit({
      if(file.exists(filepath)) file.remove(filepath)
    })
    data.table::fwrite(dataset, filepath, sep = '\t')
  }
  if(!is.null(user) && is.null(pass)) {
    stop("Must have the pass variable if the user variable is set!")
  }

  if(!is.null(user)) {
    user <- glue::glue("-U {user}")
    G <- "-G"
    DT <- "-D"
  } else {
    user <- ""
    G <- ""
    DT <- "-T"
  }
  if(!is.null(pass)) {
    pass <- glue::glue("-P {pass}")
  } else {
    pass <- ""
  }
  if(server == "PHClaims") {
    server <- "KCITSQLPRPENT40"
  }

  # Set up BCP arguments and run BCP
  bcp_args <- c(glue::glue('{schema_name}.{table_name} IN ',
                           '"{filepath}" ',
                           '-r \\n ',
                           '-t \\t ',
                           '-C 65001 ',
                           '-F 2 ',
                           '-S "{server}" ',
                           '-d {db_name} ',
                           '-b 100000 ',
                           '-c ',
                           '{G} ',
                           '{user} ',
                           '{pass} ',
                           '-q ',
                           '{DT}'))


  # Load
  bcp_output = system2(command = "bcp", args = c(bcp_args), stdout = TRUE, stderr = TRUE)

  status = attr(bcp_output, 'status')
  if(length(status)>0 && status == 1){
    stop(bcp_output)
  }

  bcp_output
}
