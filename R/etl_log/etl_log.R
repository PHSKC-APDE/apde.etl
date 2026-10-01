etl_pipeline_step <- function(pipeline_step_id = NULL,
                              pipeline_id = NULL,
                              pipeline_step_name = NULL,
                              pipeline_step_order = NULL,
                              pipeline_step_desc = NULL,
                              substeps = NULL,
                              return = T) {
  etl_log_table_check()
  ## GET CONFIG SET VARIABLES
  config <- yaml::read_yaml(file.path(here::here(), "R/etl_log/config.yaml"))
  server <- config$server
  interactive_auth <- config$interactive_auth
  prod <- config$prod
  etl_schema <- config$schema
  pipeline_table <- config$pipeline_table
  pipeline_step_table <- config$pipeline_step_table
  conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)

  ## CHECK FUNCTION VARIABLES
  if(is.null(pipeline_step_id) && (is.null(pipeline_id) || is.null(pipeline_step_name))) {
      stop("In order to create a new pipeline step, pipeline_id and pipeline_step_name must be defined.
           Updating a pipeline step requires the pipeline_step_id.")
  }
  else {

      y <- DBI::dbGetQuery(conn,
                           glue::glue_sql("SELECT * FROM {`etl_schema`}.{`pipeline_step_table`}
                                           WHERE pipeline_id = {pipeline_id}
                                           ORDER BY pipeline_step_order;",
                                          .con = conn))
      if(nrow(y) == 0 && is.null(pipeline_step_name)) {
        stop("In order to create a new pipeline step, pipeline_id and pipeline_step_name must be defined.
           Updating a pipeline step requires either a pipeline_step_id or a pipeline_id and a pipeline_step_name or pipeline_step_order.")
      } else if(nrow(y) > 0 && is.null(pipeline_step_name) && is.null(pipeline_step_order)) {
        stop("In order to create a new pipeline step, pipeline_id and pipeline_step_name must be defined.
           Updating a pipeline step requires pipeline_step_id.")
      }
  }


  ## UPDATE EXISTING PIPELINE STEP
  if(!is.null(pipeline_step_id)) {
    pipeline_step <- DBI::dbGetQuery(conn,
                                     glue::glue_sql("SELECT TOP(1) * FROM {`etl_schema`}.{`pipeline_step_table`}
                                                     WHERE pipeline_step_id = {pipeline_step_id};",
                                                     .con = conn))
    if(nrow(pipeline_step_id) == 0) {
      stop("Invalid pipeline_step_id.")
    } else {
      vars <- list()
      if(!is.null(pipeline_id)) {
        vars$pipeline_id <- pipeline_id
        to_pipeline_id <- pipeline_id
      } else {
        to_pipeline_id <- pipeline_step$pipeline_id
      }
      x <- DBI::dbGetQuery(conn,
                           glue::glue_sql("SELECT TOP(1) * FROM {`etl_schema`}.{`pipeline_table`}
                                             WHERE pipeline_id = {to_pipeline_id};",
                                          .con = conn))
      if(nrow(x) == 0) {
        stop("Invalid pipeline_id.")
      }
      if(!is.null(pipeline_step_name)) {
        vars$pipeline_step_name <- pipeline_step_name
        to_step_name <- pipeline_step_name
      } else {
        to_step_name <- pipeline_step$pipeline_step_name
      }
      x <- DBI::dbGetQuery(conn,
                           glue::glue_sql("SELECT * FROM {`etl_schema`}.{`pipeline_step_table`}
                                           WHERE pipeline_step_id <> {pipeline_step_id}
                                            AND pipeline_id = {to_pipeline_id}
                                            AND pipeline_step_name = {to_pipeline_step_name};",
                                          .con = conn))
      if(nrow(x) > 0) {
        stop("A pipeline step for the same pipeline with the same step name already exists. Please change the step name of one of the pipeline steps.")
      }
      if(!is.null(pipeline_order)) {
        vars$pipeline_order <- pipeline_order
      }
      if(!is.null(pipeline_step_desc)) {
        vars$pipelin_stepe_desc <- pipeline_step_desc
      }
      if(!is.null(substeps)) {
        if(is.list(substeps)) {
          substeps <- jsonlite::toJSON(substeps, pretty = T, auto_unbox = T)
        }
        if(!jsonlite::validate(substeps)) {
          stop("The substeps' JSON cannot be validated.")
        }
        vars$substeps <- substeps
      }
      if(length(vars) > 0) {
        DBI::dbExecute(conn,
                       glue::glue_sql("UPDATE {`etl_schema`}.{`pipeline_step_table`}
                                      SET {DBI::SQL(
                                        glue::glue_collapse(
                                          glue::glue_sql('{`names(vars)`} = {vars}', .con = conn),
                                        sep = ', \n')
                                      )}, modified_dt = GETDATE()
                                      WHERE pipeline_step_id = {pipeline_step_id};", .con = conn))
      }
      pipeline_step <- DBI::dbGetQuery(conn,
                                  glue::glue_sql("SELECT TOP(1) * FROM {`etl_schema`}.{`pipeline_step_table`}
                                             WHERE pipeline_step_id = {pipeline_step_id};",
                                                 .con = conn))
      message("Pipeline Updated...")
    }
  } else {
    ## CREATE NEW PIPELINE IF NO EXISTING PIPELINE FOUND
    if(is.null(pipeline_id)) {
      stop("The pipeline_id must be defined in order to create a new pipeline step.")
    }
    if(is.null(pipeline_step_name)) {
      stop("The pipeline_step_name must be defined in order to create a new pipeline step.")
    }
    if(is.null(pipeline_order)) {
      pipeline_order <- DBI::dbGetQuery(conn,
                            glue::glue_sql("SELECT ISNULL(MAX(pipeline_step_order), 0) AS maxOrder
                                            FROM {`etl_schema`}.{`pipeline_step_table`}
                                            WHERE pipeline_id = {pipeline_id}",
                                            .con = conn))[1,1] + 1

    } else {

    }
    if(!is.null(substeps)) {
      if(is.list(substeps)) {
        substeps <- jsonlite::toJSON(substeps, pretty = T, auto_unbox = T)
      }
      if(!jsonlite::validate(substeps)) {
        stop("The substeps' JSON cannot be validated.")
      }
    }
    pipeline_step <- DBI::dbGetQuery(conn,
                                glue::glue_sql(
                                  "INSERT INTO {`etl_schema`}.{`pipeline_step_table`}
                              (pipeline_id,
                              pipeline_step_name,
                              pipeline_step_order,
                              pipeline_step_desc,
                              substeps,
                              created_dt,
                              modified_dt)
                              OUTPUT INSERTED.*
                              VALUES
                              ({pipeline_id},
                              {pipeline_step_name},
                              {pipeline_step_order},
                              {pipeline_step_desc},
                              {substeps},
                              GETDATE(),
                              GETDATE())",
                                  .con = conn))
    message("Pipeline Created...")
  }
  if(return) {
    return(pipeline_step)
  }
}

etl_pipeline_lookup <- function(pipeline_id = NULL,
                          pipeline_datasource = NULL,
                          pipeline_name = NULL,
                          pipeline_medallion = NULL,
                          pipeline_desc = NULL,
                          pipeline_owner = NULL,
                          pipeline_status = NULL,
                          or = F,
                          steps = F) {
  etl_log_table_check()
  ## GET CONFIG SET VARIABLES
  config <- yaml::read_yaml(file.path(here::here(), "R/etl_log/config.yaml"))
  server <- config$server
  interactive_auth <- config$interactive_auth
  prod <- config$prod
  etl_schema <- config$schema
  pipeline_table <- config$pipeline_table
  conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)


  vars <- list()
  if(!is.null(pipeline_id)) {
    vars$pipeline_id <- pipeline_id
  }
  if(!is.null(pipeline_datasource)) {
    vars$pipeline_datasource <- pipeline_datasource
  }
  if(!is.null(pipeline_name)) {
    vars$pipeline_name <- pipeline_name
  }
  if(!is.null(pipeline_medallion)) {
    vars$pipeline_medallion <- pipeline_medallion
  }
  if(!is.null(pipeline_desc)) {
    vars$pipeline_desc <- pipeline_desc
  }
  if(!is.null(pipeline_owner)) {
    vars$pipeline_owner <- pipeline_owner
  }
  if(!is.null(pipeline_status)) {
    vars$pipeline_status <- pipeline_status
  }
  if(length(vars) > 0) {
    if(or == T) {
      sep <- " OR \n"
    } else {
      sep <- " AND \n"
    }
    pipelines <- DBI::dbGetQuery(conn,
                   glue::glue_sql("SELECT * FROM {`etl_schema`}.{`pipeline_table`}
                                    WHERE {DBI::SQL(
                                        glue::glue_collapse(
                                          glue::glue_sql('{`names(vars)`} = {vars}', .con = conn),
                                        sep = {sep})
                                      )} ORDER BY pipeline_datasource, pipeline_name;", .con = conn))
  } else {
    stop("At least one variable must be defined to lookup a pipeline.")
  }
  return(pipelines)
}

etl_pipeline <- function(pipeline_id = NULL,
                         pipeline_datasource = NULL,
                         pipeline_name = NULL,
                         pipeline_medallion = NULL,
                         pipeline_desc = NULL,
                         pipeline_owner = NULL,
                         pipeline_status = NULL,
                         return = T) {

  etl_log_table_check()
  ## GET CONFIG SET VARIABLES
  config <- yaml::read_yaml(file.path(here::here(), "R/etl_log/config.yaml"))
  server <- config$server
  interactive_auth <- config$interactive_auth
  prod <- config$prod
  etl_schema <- config$schema
  pipeline_table <- config$pipeline_table
  conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)

  ## CHECK FUNCTION VARIABLES
  if(is.null(pipeline_id) && (is.null(pipeline_name) || is.null(pipeline_datasource))) {
    stop("In order to create a new pipeline, pipeline_datasource AND pipeline_name must be defined.
         Updating a pipeline requires a pipeline_id.")
  }

  ## UPDATE EXISTING PIPELINE
  if(!is.null(pipeline_id)) {
    pipeline <- DBI::dbGetQuery(conn,
                                glue::glue_sql("SELECT TOP(1) * FROM {`etl_schema`}.{`pipeline_table`}
                                             WHERE pipeline_id = {pipeline_id};",
                                               .con = conn))
    if(nrow(pipeline) == 0) {
      stop("Invalid pipeline_id.")
    } else {
      vars <- list()
      if(!is.null(pipeline_datasource)) {
        vars$pipeline_datasource <- pipeline_datasource
        to_datasource <- pipeline_datasource
      } else {
        to_datasource <- pipeline$pipeline_datasource
      }
      if(!is.null(pipeline_name)) {
        vars$pipeline_name <- pipeline_name
        to_name <- pipeline_name
      } else {
        to_name <- pipeline$pipeline_name
      }
      if(!is.null(pipeline_medallion)) {
        vars$pipeline_medallion <- pipeline_medallion
      }
      if(!is.null(pipeline_desc)) {
        vars$pipeline_desc <- pipeline_desc
      }
      if(!is.null(pipeline_owner)) {
        vars$pipeline_owner <- pipeline_owner
      }
      if(!is.null(pipeline_status)) {
        vars$pipeline_status <- pipeline_status
        to_status <- pipeline_status
      } else {
        to_status <- pipeline$pipeline_status
      }
      ## CHECK FOR PIPELINE WITH THE SAME NAME FOR THE SAME DATASOURCE THAT IS ACTIVE
      x <- DBI::dbGetQuery(conn,
                           glue::glue_sql("SELECT * FROM {`etl_schema`}.{`pipeline_table`}
                                        WHERE pipeline_datasource = {to_datasource}
                                          AND pipeline_name = {to_name}
                                          AND pipeline_status = {to_status}
                                          AND pipeline_id <> {pipeline$pipeline_id}",
                                          .con = conn))
      if(nrow(x) > 0 && to_status == "ACTIVE") {
        stop("A pipeline with the same name, datasource and ACTIVE status already exists. Please change the name or status of one of the pipelines.")
      }
      if(length(update) > 0) {
        DBI::dbExecute(conn,
                       glue::glue_sql("UPDATE {`etl_schema`}.{`pipeline_table`}
                                      SET {DBI::SQL(
                                        glue::glue_collapse(
                                          glue::glue_sql('{`names(vars)`} = {vars}', .con = conn),
                                        sep = ', \n')
                                      )}, modified_dt = GETDATE()
                                      WHERE pipeline_id = {pipeline_id};", .con = conn))
      }

      pipeline <- DBI::dbGetQuery(conn,
                                  glue::glue_sql("SELECT TOP(1) * FROM {`etl_schema`}.{`pipeline_table`}
                                             WHERE pipeline_id = {pipeline_id};",
                                                 .con = conn))
      message("Pipeline Updated...")
    }
  } else {
    ## CREATE NEW PIPELINE IF NO EXISTING PIPELINE FOUND
    if(is.null(pipeline_datasource)) {
      stop("The pipeline_datasource must be defined in order to create a new pipeline.")
    }
    if(is.null(pipeline_name)) {
      stop("The pipeline_name must be defined in order to create a new pipeline.")
    }
    if(is.null(pipeline_desc)) {
      stop("The pipeline_desc must be defined in order to create a new pipeline.")
    }
    if(is.null(pipeline_owner)) {
      pipeline_owner <- Sys.info()[["user"]]
    }
    if(is.null(pipeline_status)) {
      pipeline_status <- "ACTIVE"
    }
    x <- DBI::dbGetQuery(conn,
                         glue::glue_sql("SELECT * FROM {`etl_schema`}.{`pipeline_table`}
                                        WHERE pipeline_datasource = {pipeline_datasource}
                                          AND pipeline_name = {pipeline_name}
                                          AND pipeline_status = {pipeline_status}",
                                        .con = conn))
    if(nrow(x) > 0 && pipeline_status == "ACTIVE") {
      stop("A pipeline with the same name, datasource and ACTIVE status already exists. Please change the name or status of one of the pipelines.")
    }
    pipeline <- DBI::dbGetQuery(conn,
                                glue::glue_sql(
                                  "INSERT INTO {`etl_schema`}.{`pipeline_table`}
                              (pipeline_datasource,
                              pipeline_name,
                              pipeline_medallion,
                              pipeline_desc,
                              pipeline_owner,
                              pipeline_status,
                              created_dt,
                              modified_dt)
                              OUTPUT INSERTED.*
                              VALUES
                              ({pipeline_datasource},
                              {pipeline_name},
                              {pipeline_medallion},
                              {pipeline_desc},
                              {pipeline_owner},
                              {pipeline_status},
                              GETDATE(),
                              GETDATE())",
                                  .con = conn))
    message("Pipeline Created...")
  }
  if(return) {
    return(pipeline)
  }
}

#' @title Check For and Create ETL Log SQL tables
#'
#' @description
#' Check if ETL log tables exist and build if not.
#'
#' @details
#' Checks for the existence of the 4 ETL log tables. If the tables exist, their
#' structure is compared to the YAML configuration file. If the structure is
#' different, old tables are archived and a new tables are created. If a table
#' does not already exist, the table is created. If all tables exist with
#' the correct structure, nothing happens
#'
#' @note
#' This function assumes all ETL logging will occur on the production
#' version of the hhs_analytics_workspace Azure SQL database.
#'
#'
#' @return None (invisible NULL)
#'
#' @export
#'
#'
etl_log_table_check <- function() {
  ## GET CONFIG SET VARIABLES
  config <- yaml::read_yaml(file.path(here::here(), "R/etl_log/config.yaml"))
  server <- config$server
  interactive_auth <- config$interactive_auth
  prod <- config$prod
  ts <- format(Sys.time(), format = "%Y%m%d_%H%M%S")
  archive_ts <- paste0("_archive_", ts)
  to_schema <- config$schema
  ## KEYRING CHECK
  if(length(keyring::key_list(server)[["username"]]) == 0) {
    stop(paste0("No Key Ring has been set for SERVER: [", server, "]!"))
  }
  ## CREATING DB CONNECTION
  conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  ## CREATE GET COLUMN FUNCTION
  get_vars <- function(schema_name, table_name) {
    cols <- DBI::dbGetQuery(conn,
                            glue::glue_sql("SELECT
                                            [COLUMN_NAME],
                                            CONCAT(
                                            UPPER([DATA_TYPE]),
	                                            CASE
		                                            WHEN [DATA_TYPE] IN('VARCHAR', 'CHAR', 'NVARCHAR') THEN CONCAT('(',CASE
		                                            WHEN [CHARACTER_MAXIMUM_LENGTH] = -1 THEN 'MAX'
		                                            ELSE CAST([CHARACTER_MAXIMUM_LENGTH] AS VARCHAR(4))
		                                            END
		                                            , ')')
		                                            WHEN [DATA_TYPE] IN('DECIMAL', 'NUMERIC') THEN CONCAT('(', [NUMERIC_PRECISION], ',', [NUMERIC_SCALE], ')')
		                                            ELSE ''
	                                            END) AS 'COLUMN_DEF'
                                            FROM [INFORMATION_SCHEMA].[COLUMNS]
                                            WHERE [TABLE_NAME] = {table_name} AND [TABLE_SCHEMA] = {schema_name}
                                            ORDER BY [ORDINAL_POSITION]",
                                           .con = conn))
    vars <- list()
    for(i in 1:nrow(cols)) {
      vars[cols[i,"COLUMN_NAME"]] <- cols[i,"COLUMN_DEF"]
    }
    return(vars)
  }
  ## CHECK PIPELINE TABLE
  to_table <- config$pipeline_table
  if (DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
    vars <- get_vars(to_schema, to_table)
    if(!identical(vars, config$pipeline_vars)) {
      message(glue::glue("[{to_schema}].[{to_table}] - ETL Pipeline Table Structure has Changed!"))
      message(glue::glue("[{to_schema}].[{to_table}] - Archiving Old Table ({paste0(to_table, archive_ts)})."))
      tryCatch(
        {
          DBI::dbExecute(conn,
                         glue::glue_sql("EXEC sp_rename {paste0(to_schema, '.', to_table)},
                                          {paste0(to_table, archive_ts)}",
                                        .con = conn))
        },
        error = function(cond) {
          DBI::dbExecute(conn_to,
                         glue::glue_sql("RENAME OBJECT {`to_schema`}.{`to_table`}
                                          TO {`{paste0(to_table, archive_ts)}`}",
                                        .con = conn_to))
        }
      )
    }
  } else {
    message(glue::glue("[{to_schema}].[{to_table}] - ETL Pipeline Table Does Not Exist!"))
  }
  if (!DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
    create_table(conn,
                 server = "hhsaw",
                 to_schema = to_schema,
                 to_table = to_table,
                 vars = config$pipeline_vars,
                 overwrite = F)
    DBI::dbExecute(conn,
                   glue::glue_sql(
                     "ALTER TABLE {`to_schema`}.{`to_table`}
                     ADD CONSTRAINT DF_pipeline_id_{DBI::SQL(ts)} DEFAULT NEWID() FOR pipeline_id;",
                     .con = conn
                   ))
  }
  ## CHECK PIPELINE STEP TABLE
  to_table <- config$pipeline_step_table
  if (DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
    vars <- get_vars(to_schema, to_table)
    if(!identical(vars, config$pipeline_step_vars)) {
      message(glue::glue("[{to_schema}].[{to_table}] - ETL Pipeline Step Table Structure has Changed!"))
      message(glue::glue("[{to_schema}].[{to_table}] - Archiving Old Table ({paste0(to_table, archive_ts)})."))
      tryCatch(
        {
          DBI::dbExecute(conn,
                         glue::glue_sql("EXEC sp_rename {paste0(to_schema, '.', to_table)},
                                          {paste0(to_table, archive_ts)}",
                                        .con = conn))
        },
        error = function(cond) {
          DBI::dbExecute(conn_to,
                         glue::glue_sql("RENAME OBJECT {`to_schema`}.{`to_table`}
                                          TO {`{paste0(to_table, archive_ts)}`}",
                                        .con = conn_to))
        }
      )
    }
  } else {
    message(glue::glue("[{to_schema}].[{to_table}] - ETL Pipeline Step Table Does Not Exist!"))
  }
  if (!DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
    create_table(conn,
                 server = "hhsaw",
                 to_schema = to_schema,
                 to_table = to_table,
                 vars = config$pipeline_step_vars,
                 overwrite = F)
    DBI::dbExecute(conn,
                   glue::glue_sql(
                     "ALTER TABLE {`to_schema`}.{`to_table`}
                     ADD CONSTRAINT DF_pipeline_step_id_{DBI::SQL(ts)} DEFAULT NEWID() FOR pipeline_step_id;",
                     .con = conn
                   ))
  }
  ## CHECK RUN LOG TABLE
  to_table <- config$log_table
  if (DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
    vars <- get_vars(to_schema, to_table)
    if(!identical(vars, config$log_vars)) {
      message(glue::glue("[{to_schema}].[{to_table}] - ETL Run Log Table Structure has Changed!"))
      message(glue::glue("[{to_schema}].[{to_table}] - Archiving Old Table ({paste0(to_table, archive_ts)})."))
      tryCatch(
        {
          DBI::dbExecute(conn,
                         glue::glue_sql("EXEC sp_rename {paste0(to_schema, '.', to_table)},
                                          {paste0(to_table, archive_ts)}",
                                        .con = conn))
        },
        error = function(cond) {
          DBI::dbExecute(conn_to,
                         glue::glue_sql("RENAME OBJECT {`to_schema`}.{`to_table`}
                                          TO {`{paste0(to_table, archive_ts)}`}",
                                        .con = conn_to))
        }
      )
    }
  } else {
    message(glue::glue("[{to_schema}].[{to_table}] - ETL Run Log Table Does Not Exist!"))
  }
  if (!DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
    create_table(conn,
                 server = "hhsaw",
                 to_schema = to_schema,
                 to_table = to_table,
                 vars = config$log_vars,
                 overwrite = F)
    DBI::dbExecute(conn,
                   glue::glue_sql(
                     "ALTER TABLE {`to_schema`}.{`to_table`}
                     ADD CONSTRAINT DF_etl_id_{DBI::SQL(ts)} DEFAULT NEWID() FOR etl_id;",
                     .con = conn
                   ))
  }
  ## CHECK RUN LOG STEP TABLE
  to_table <- config$log_step_table
  if (DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
    vars <- get_vars(to_schema, to_table)
    if(!identical(vars, config$log_step_vars)) {
      message(glue::glue("[{to_schema}].[{to_table}] - ETL Run Log Step Table Structure has Changed!"))
      message(glue::glue("[{to_schema}].[{to_table}] - Archiving Old Table ({paste0(to_table, archive_ts)})."))
      tryCatch(
        {
          DBI::dbExecute(conn,
                         glue::glue_sql("EXEC sp_rename {paste0(to_schema, '.', to_table)},
                                          {paste0(to_table, archive_ts)}",
                                        .con = conn))
        },
        error = function(cond) {
          DBI::dbExecute(conn_to,
                         glue::glue_sql("RENAME OBJECT {`to_schema`}.{`to_table`}
                                          TO {`{paste0(to_table, archive_ts)}`}",
                                        .con = conn_to))
        }
      )
    }
  } else {
    message(glue::glue("[{to_schema}].[{to_table}] - ETL Run Log Step Table Does Not Exist!"))
  }
  if (!DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
    create_table(conn,
                 server = "hhsaw",
                 to_schema = to_schema,
                 to_table = to_table,
                 vars = config$log_step_vars,
                 overwrite = F)
    DBI::dbExecute(conn,
                   glue::glue_sql(
                     "ALTER TABLE {`to_schema`}.{`to_table`}
                     ADD CONSTRAINT DF_etl_step_id_{DBI::SQL(ts)} DEFAULT NEWID() FOR etl_step_id;",
                     .con = conn
                   ))
  }
  DBI::dbDisconnect(conn)
}
