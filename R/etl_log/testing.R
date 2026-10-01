library(jsonlite)
library(apde.etl)
etl_log_table_check()

config <- yaml::read_yaml(file.path(here::here(), "R/etl_log/config.yaml"))
server <- config$server
interactive_auth <- config$interactive_auth
prod <- config$prod
ts <- format(Sys.time(), format = "%Y%m%d-%H%M%S")
archive_ts <- paste0("_archive_", ts)
to_schema <- config$schema
conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)

old_p <- pipeline

####
pipeline_id = old_p$pipeline_id
pipeline_datasource = NULL
pipeline_name = NULL
pipeline_medallion = "plat"
pipeline_desc = "Identify new files, check structure and data for correct date ranges and proper valuesz."
pipeline_owner = NULL
pipeline_status = NULL
####
pipeline_id = NULL
pipeline_datasource = "CHARS"
pipeline_name = "Raw File Loading"
pipeline_medallion = "silver"
pipeline_desc = "Load raw files, clean and combine with other raw data."
pipeline_owner = NULL
pipeline_status = NULL


rm(x)
x <- etl_pipeline(
  pipeline_datasource = "CHARS",
  pipeline_name = NULL,
)

etl_pipeline(
  pipeline_id = old_p$pipeline_id,
  pipeline_datasource = NULL,
  pipeline_name = NULL,
  pipeline_medallion = "plat",
  pipeline_desc = "Identify new files, check structure and data for correct date ranges and proper valuesz.",
  pipeline_owner = NULL,
  pipeline_status = NULL
)

etl_pipeline(
  pipeline_id = NULL,
  pipeline_datasource = NULL,
  pipeline_name = NULL,
  pipeline_medallion = NULL,
  pipeline_desc = NULL,
  pipeline_owner = NULL,
  pipeline_status = NULL,
  overwrite = T
)

b <- etl_pipeline(
  pipeline_datasource = "CHARS",
  pipeline_name = "Raw File Processing",
  pipeline_medallion = "bronze",
  pipeline_desc = "Review raw files for correct structure, data ranges and new columns."
)
s <- etl_pipeline(
  pipeline_datasource = "CHARS",
  pipeline_name = "Raw File Loading",
  pipeline_medallion = "silver",
  pipeline_desc = "Load raw files, clean and combine with other raw data.",
  pipeline_status = "DEACTIVATED"
)
etl_pipeline_lookup(
             pipeline_name = "Raw File Loading",
             pipeline_status = "ACTIVE",
             or = T
)

ex_etl <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/GitHub/DOHdata/ETL/chars/etl_config.yaml")
ex_substeps <- ex_etl$steps[[1]]$substeps
#ex_bad_substeps <- yaml::read_yaml("C:/Users/jwhitehurst/OneDrive - King County/GitHub/DOHdata/ETL/chars/bad_etl_config.yaml")
json <- jsonlite::toJSON(ex_substeps, pretty = T, auto_unbox = T)
validate(json)

is.list(ex_substeps)
