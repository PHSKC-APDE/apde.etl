# apde.etl
APDE Tools for ETL (Extract, Transform, & Load) Processes

----------------------------

## Purpose
This package provides a variety of tools to facilitate APDE's ETL and informatics processes. For example, there are functions to create SQL server connections, and load, create, copy, and QA tables. See the [Highlighted functions](#highlighted-functions-in-alphabetical-order) section below for more details. 

For APDE's custom *analytic tools*, please refer to the [rads](https://github.com/PHSKC-APDE/rads/) package. 

## Installation
1.  `apde.etl` depends on version 17 or higher of [Microsoft ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15). You can download it [here](https://go.microsoft.com/fwlink/?linkid=2187214)

2.  Make sure remotes is installed in R ... 
    - `install.packages("remotes")`.

3.  Install `apde.etl` in R ... 
    - `remotes::install_github("PHSKC-APDE/apde.etl", auth_token = NULL)`

4.  Load `apde.etl` in R ... 
    - `library(apde.etl)`

#### Optional (but recommended)

 - `install.packages("keyring")`
 -  `keyring::key_set(service = 'hhsaw', username = 'Your.KCUsername@kingcounty.gov')`
 - In the pop-up window type in your standard King County password (the one you use to log into your laptop) and click `OK`

#### Note!!

 - You will need permission to access the HHSAW Production SQL Server for many of the functions. 

## Highlighted functions (in alphabetical order)

### `add_index()`
 Add an index to a SQL Server table. 

### `apde_keyring_set()`
Create or update a keyring with a specified username.

### `apde_keyring_check()`
Check if a keyring exists and create it if it does not exist.

### `apde_notify_set_cred()`
Reset the saved Outlook credentials stored on the system's keyring. This function will ask for an email address and password.

### `apde_notify_menu()`
Launches an interactive Shiny web application for creating, editing, and managing email notification templates and recipient lists. This is the primary interface for setting up the notification system before using `apde_notify()` to send emails. 

### `apde_notify(msg_id, msg_name, vars)`
Sends an email notification to a predefined list of recipients using a stored message template.

### `check_version()`
Check if the installed apde.etl package version is the latest available on GitHub.

### `copy_into()`
Copy data from the data lake to the data warehouse using SQL Server's COPY INTO statement.

### `create_db_connection()`
Create a connection to APDE's prod or dev servers.

### `deduplicate_addresses()`
Remove duplicate addresses in the ref tables and synchronize the address tables between servers.

### `etl_qa_run_pipeline()`
Run a quality assurance pipeline for ETL (Extract, Transform, Load) processes. It analyzes data for missingness, variable distributions, and optionally checks compliance with CHI (Community Health Indicators) standards.

### `external_table_check()`
Compare an external table to a source table to identify changes and generate recreation scripts when needed.

### `load_df_bcp()`
Load an R data.frame to SQL Server using [BCP (Bulk Copy Program)](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16&tabs=windows).

### `load_table_from_file()`
Load file data to a SQL table using specified variables or a YAML config file.

### `load_table_from_sql()`
Load data from one SQL table to another using specified variables or a YAML config file.

### `table_duplicate()`
Copy a (smaller) SQL table from one server to another.

### `table_duplicate_delete()`
Delete tables based on their suffix pattern.

## Problems?

-   If you encounter a bug or have specific suggestions for improvement, please click on ["Issues"](https://github.com/PHSKC-APDE/apde.etl/issues) at the top of this page and then click ["New Issue"](https://github.com/PHSKC-APDE/apde.etl/issues/new/choose) and provide the necessary details.