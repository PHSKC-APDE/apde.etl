library('knitr')
library('quarto')

# Start in apde.etl directory
start_dir <- getwd()

# Find all Qmd files
qmds <- normalizePath(list.files('quarto_docs', pattern = '\\.qmd$', full.names = TRUE, ignore.case = TRUE))

# Set up temporary directory and clone wiki repo
td <- tempdir()
setwd(td)
wiki_dir <- file.path(td, 'apde.etl.wiki')
system('git clone https://github.com/PHSKC-APDE/apde.etl.wiki.git')
setwd(wiki_dir)

# Knit / Render each file
for(qmd_file in qmds) {
  cat("Processing:", qmd_file, "\n")

  # Designate output filename
    out_file <- paste0(tools::file_path_sans_ext(basename(qmd_file)), '.md')

  # Render file
    quarto::quarto_render(
      input = qmd_file,
      output_format = "gfm",
      output_file = out_file
    )

  # Clear out YAML headers
  if(file.exists(out_file)) {
    rl <- readLines(out_file)
    if(length(rl) > 0 && rl[1] == '---') {
      end_yaml <- which(rl == '---')[2]
      if(!is.na(end_yaml)) {
        rl <- rl[(end_yaml+1):length(rl)]
        writeLines(rl, con = out_file)
      }
    }
  } else {
    warning(paste("Output file not created:", out_file))
  }
}

# Push changes to https://github.com/PHSKC-APDE/apde.etl.wiki.git
system('git add .')
system('git commit -m "Update wiki from automated process"')
system('git push origin master')


# Clean up
setwd(start_dir)
gc()
unlink(wiki_dir, recursive = TRUE, force = TRUE)
rm(wiki_dir)
