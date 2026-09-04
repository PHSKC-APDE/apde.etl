# Author: Danny Colombara
# R version: 4.6.1
# Date: September 4, 2026
# Purpose: Generate chi_standard_varnames reference table from SharePoint
# Source: SharePoint >> DPH-CHI >> CHI-Vizes >> CHI-Standards-TableauReady Output.xlsx
# Notes: This dataset provides the definitive reference for CHI cat/varname/group values

# Create function ----
update_chi_standard_varnames <- function() {

  # Helper function to identify changes ----
  validate_changes <- function(new_data, existing_data) {
    # Compare first 5 columns (excluding creation_date)
    new_core <- new_data[, 1:5]
    existing_core <- existing_data[, 1:5]

    added <- data.table::fsetdiff(new_core, existing_core)
    removed <- data.table::fsetdiff(existing_core, new_core)

    has_changes <- nrow(added) > 0 || nrow(removed) > 0

    if (!has_changes) {
      message('No changes detected. Dataset will not be updated.')
      return(FALSE)
    }

    # Display changes
    if (nrow(added) > 0) {
      message("\n=== ROWS TO BE ADDED ===")
      print(added)
    }

    if (nrow(removed) > 0) {
      message("\n=== ROWS TO BE REMOVED ===")
      print(removed)
    }

    return(TRUE)
  }

  # Import data from SharePoint ----
  message("Connecting to SharePoint...")
  team <- Microsoft365R::get_team("DPH-CHI")
  drv <- team$get_drive("CHI-Vizes")

  tempy <- tempfile(fileext = ".xlsx")

  drv$download_file(
    src = 'CHI-Standards-TableauReady Output.xlsx',
    dest = tempy,
    overwrite = TRUE
  )

  chi_standard_varnames_new <- data.table::setDT(openxlsx::read.xlsx(tempy, sheet = "Standard-Varname_Groups"))
  chi_standard_varnames_new <- chi_standard_varnames_new[, list(cat, varname, group, keepme, notes)]

  unlink(tempy) # delete temporary object

  # Clean and process data ----

  # Remove misc whitespace
  rads::string_clean(chi_standard_varnames_new)

  # Replace irregular whitespaces with standard spaces
  string_columns <- names(chi_standard_varnames_new)
  chi_standard_varnames_new[, (string_columns) := lapply(.SD, function(x) {
    stringi::stri_replace_all_charclass(x, "\\p{WHITE_SPACE}", " ")
  }), .SDcols = string_columns]

  # Replace quotation marks in notes column
  chi_standard_varnames_new[, notes := gsub('"', "`", notes)]

  # Sort: KC first, then WA State, then alphabetically by cat
  data.table::setorder(chi_standard_varnames_new, cat, varname, group)
  chi_standard_varnames_new <- rbind(
    chi_standard_varnames_new[cat == 'King County'],
    chi_standard_varnames_new[cat == "Washington State"],
    chi_standard_varnames_new[!cat %in% c("King County", "Washington State")]
  )

  # Add creation date
  chi_standard_varnames_new[, creation_date := Sys.Date()]

  # Validate changes ----
  message("\nChecking for changes vs previous run ...")

  # Try to load existing data from the package
  if (file.exists("data/chi_standard_varnames.rda")) {
    message("Loaded existing data from data/chi_standard_varnames.rda")
    load("data/chi_standard_varnames.rda")
    chi_standard_varnames_existing <- data.table::copy(chi_standard_varnames)
    rm(chi_standard_varnames)

    data.table::setorder(chi_standard_varnames_existing, cat, varname, group)

    has_changes <- validate_changes(chi_standard_varnames_new, chi_standard_varnames_existing)

    if (!has_changes) {
      message("\U1F6D1 No changes detected. Exiting without updating.")
      return(invisible())
    } else {
      message("\nChanges detected. Proceeding with update...")
    }

    # Save as .rda file for package
    chi_standard_varnames <- data.table::copy(chi_standard_varnames_new)
    usethis::use_data(chi_standard_varnames, compress = "bzip2", version = 3, overwrite = TRUE)
    message("✓ Saved to data/chi_standard_varnames.rda")
    message("\nDataset successfully updated with ", nrow(chi_standard_varnames), " rows\n",
            "=== NEXT STEPS ===\n",
            "1. Review the changes displayed above\n",
            "2. Update documentation in R/data.R if needed\n",
            "3. Run devtools::document()\n",
            "4. Run devtools::check()\n",
            "5. Commit changes to your feature branch")
  } else {
    stop('\n\U1F6D1 The previous version was not found in data/chi_standard_varnames.rda')
  }
}

# Run function ----
update_chi_standard_varnames()
# The end ----
