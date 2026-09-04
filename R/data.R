# chi_standard_varnames ----
#' The definitive reference table for CHI cat / varname / group values
#'
#' The definitive reference table for CHI cat / varname / group values.
#' Used in CHI analyses as the by-variables (cat1 & cat2 in CHI parlance).
#'
#' @format A data.table with 6 columns and a flexible number of rows:
#' - `cat`: Category name
#' - `varname`: Variable name
#' - `group`: Group identifier
#' - `keepme`: Logical flag indicating whether to retain this row
#' - `notes`: Additional notes or comments
#' - `creation_date`: Date the dataset was generated
#'
#' @details This dataset replaces `rads.data::misc_chi_byvars` (deprecated).
#' The data is sorted with King County first, Washington State second, then
#' alphabetically by category.
#'
#' @source A version-controlled copy of data from SharePoint:
#' [DPH-CHI > CHI_vizes > CHI-Standards-TableauReady Output.xlsx](<https://kc1.sharepoint.com/teams/DPH-CHI/CHIVizes/CHI-Standards-TableauReady Output.xlsx>).
#'
#' @note This replaces any and all old standards found in documentation or in
#' existing SQL databases.
#'
#' @examples
#' \dontrun{
#' # Load the standard varnames
#' data(chi_standard_varnames)
#'
#' # View King County categories
#' chi_standard_varnames[cat == "King County"]
#'
#' # Get unique categories
#' unique(chi_standard_varnames$cat)
#' }
#'
#' @keywords datasets
#' @name chi_standard_varnames
"chi_standard_varnames"
