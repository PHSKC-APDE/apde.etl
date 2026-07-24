# generate_yaml() ----
#' Generate a YAML file for SQL loading based on in a data.frame or data.table
#'
#' @description
#' YAML files can be helpful for uploading data to SQL efficiently and correctly. This function should enable
#' the user to create a standard YAML file that be be used to push data from R to SQL.
#'
#' This function expects data in the form of a data.frame or data.table.
#'
#' @note Ported from the `rads` package. It has been migrated here in preparation for release of
#' `rads` version 2.0.0.
#'
#' @param mydt The name of a data.table or data.frame for which you want to create a YAML file
#' @param outfile Optional character vector of length one. The complete filepath for where the *.yaml file
#' should be saved. If it is not specified, the YAML file will be returned in memory
#' @param datasource A character vector of length one. A human readable description of the datasource to be
#' uploaded to SQL. This could be a filepath to the original data on a shared drive or a simple description.
#' @param schema A character vector of length one. The schema to be used within the specific server and
#' database that will be specified in your odbc connection.
#' @param table A character vector of length one. The table to be used within the specific server and
#' database that will be specified in your odbc connection and within the schema that you specified above.
#'
#' @details
#' The YAML output can be used to supply field types to functions such as
#' [tsql_chunk_loader()], [tsql_convert_types()], and [tsql_validate_field_types()]. For example:
#' `field_types = unlist(my_yaml$vars)`. If the YAML was written to disk using `outfile`, it can be
#' reloaded later with [yaml::read_yaml()].
#'
#' @return a list with the YAML file contents (if outfile not specified) or a message stating where the YAML
#' file has been saved (if outfile was specified)
#'
#' @export
#'
#' @keywords YAML
#'
#' @name generate_yaml
#'
#' @importFrom data.table ':=' data.table copy setDT is.data.table
#' @importFrom yaml read_yaml
#'
#' @examples
#'
#' \donttest{
#' data(mtcars)
#' # output to object in memory
#'   check <- generate_yaml(mtcars, schema = "SCH", table = "TBL",
#'   datasource = "R standard mtcars")
#'
#' # output to a file
#'   output_file <- tempfile('output_file', fileext = ".yaml")
#'   generate_yaml(mtcars, outfile = output_file, schema = "SCH", table = "TBL",
#'   datasource = "R standard mtcars")
#' }
#'
generate_yaml <- function(mydt, outfile = NULL, datasource = NULL, schema = NULL, table = NULL){

  #Bindings for data.table/check global variables
  vartype <- binary <- varname <- i <- varlength <- sql <- NULL

  mi.outfile = 0


  ## Error check ----
  if(is.null(mydt))stop("mydt, the name of a data.frame or data.table for which you wish to create a YAML file, must be provided.")

  if(!is.data.table(mydt)){
    if(is.data.frame(mydt)){
      setDT(mydt)
    } else {
      stop(paste0("<mydt> must be the name of a data.frame or data.table."))
    }
  }

  if(!is.null(outfile)){
    if(!grepl("\\.yaml$", outfile)){
      stop(paste0("The value for 'outfile' (the complete filepath for saving the YAML you are creating), \n",
                  "must have the file extension '.yaml"))
    }}

  if(is.null(outfile)){
    message(paste0("You did not submit a value for 'outfile' (the complete filepath for saving the YAML you are creating), \n",
                   "and that's okay! \n \n",
                   "To save the yaml object in memory (as a list), assign a name to the output of this function, e.g., \n",
                   "my_new_yaml <- generate_yaml(...)"))
    mi.outfile = 1
    outfile <- tempfile("blahblah", fileext = ".yaml")
  }

  if(is.null(schema)){
    stop("You must submit a SQL schema for the header of the YAML file")
  }

  if(is.null(schema)){
    stop("You must submit a SQL table name for the header of the YAML file")
  }

  if(is.null(datasource)){
    message(paste0("\nWarning: You did not enter a datasource for where the underlying data exists on a shared drive. \n",
                   "The YAML file will be created, but the datasource will not be recorded in the header."))
  }

  ## Set up ----
  # identify column type
  temp.vartype <- data.table(varname = names(sapply(mydt, class)), vartype = sapply(mydt, function(x) paste(class(x), collapse = ',')))

  # identify if it is a binary
  temp.binary <- data.table(varname = names(sapply(mydt,function(x) { all(stats::na.omit(x) %in% 0:1) })), binary = sapply(mydt,function(x) { all(stats::na.omit(x) %in% 0:1) }))

  # merge binary indicator to the column types
  mydict <- merge(temp.vartype, temp.binary, by = "varname")

  # identify vartype == binary
  mydict[vartype %in% c("numeric", "integer") & binary == TRUE, vartype := "binary"]
  mydict[, binary := NULL]

  # ensure consistent ordering
  mydict[, varname := factor(varname, levels = names(mydt))]
  setorder(mydict, varname)

  # Identify standard TSQL numeric & string types ----
  # Identify all integers << tinyint, smallint, and bigint probably should not be automatically ascribed
  potential.int <- as.character(mydict[vartype %in% c("numeric", "integer")]$varname)
  for(i in potential.int){
    mydict[varname==i & all(mydt[!is.na(get(i)), .SD, .SDcols = i] == floor(mydt[!is.na(get(i)), .SD, .SDcols = i])) == TRUE, vartype := "integer"]
    mydict[varname==i & max(mydt[[i]], na.rm = T) >= 2147483647, vartype := "numeric"]
  }

  # Set varchar (assumed 1 chars ~= 1 byte and will add buffer of 100%)
  potential.varchar <- as.character(mydict[vartype %in% c("character", "factor")]$varname)
  for(i in potential.varchar){
    mydict[varname==i, varlength := 2+ceiling(max(nchar(as.character(mydt[[i]])[!is.na(mydt[[i]])]))*2)]
    mydict[varname==i & is.na(varlength), varlength := 36] # arbitrarily chose 'n'==36 when character vector is 100% NA
  }

  # Ascribe SQL data type names ----
  sqlkey <- data.table(
    vartype = c("logical", "character", "factor", "binary", "integer", "numeric", "Date", "POSIXct,POSIXt"),
    sql = c("BIT", "NVARCHAR", "NVARCHAR", "BIT", "INT", "NUMERIC(38,5)", "DATE", "DATETIME")  # NUMERIC(38,5) ... allows for up to 38 digits of precision, with 5 of those to the right of the decimal
  )

  mydict <- merge(mydict, sqlkey, by = "vartype", all.x = TRUE, all.y = FALSE)

  ## Clean up ----
  mydict[, varname := factor(varname, levels = names(mydt))]
  mydict[sql == "NVARCHAR", sql := paste0(sql, "(", varlength, ")")]
  mydict[, sql := paste0("    ", varname, ": ", sql)]
  setorder(mydict, varname) # sort in same order as the data.table
  mydict <- mydict[, list(sql)]

  if(!is.null(datasource)){
    header <- data.table(
      sql = c(paste0("datasource: ", datasource),
              paste0("schema: ", schema),
              paste0("table: ", table),
              "vars: "))
  } else {
    header <- data.table(
      sql = c(paste0("schema: ", schema),
              paste0("table: ", table),
              "vars: "))
  }


  mydict <- rbind(header, mydict)

  # save yaml file ----
  fwrite(x = mydict,
         file = outfile,
         quote = F,
         col.names=F,
         row.names = F,
         append=F)

  MyYAML <- yaml::read_yaml(outfile)

  if(mi.outfile == 1){return(MyYAML)}else{message(paste0("YAML saved to ", outfile))}

}

