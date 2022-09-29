# Building a Prod-Ready, Robust Shiny Application.
#
# README: each step of the dev files is optional, and you don't have to
# fill every dev scripts before getting started.
# 01_start.R should be filled at start.
# 02_dev.R should be used to keep track of your development during the project.
# 03_deploy.R should be used once you need to deploy your app.
#
#
########################################
#### CURRENT FILE: ON START SCRIPT #####
########################################

renv::init()

usethis::use_description(
  fields = list(
    Title = "HadesExtras",
    Description = "Functions to complement Hades packages",
    `Authors@R` = 'person("Javier", "Gracia-Tabuenca", email = "javier.graciatabuenca@tuni.fi",
                          role = c("aut", "cre"),
                          comment = c(ORCID = "0000-0002-2455-0598"))',
    License = "none",
    Language =  "en"
  )
)

# ## Use git ----
usethis::use_git()
Sys.setenv(GITHUB_PAT = gitcreds::gitcreds_get(use_cache = FALSE)$password)
usethis::use_github(private = TRUE)
usethis::use_readme_md()
