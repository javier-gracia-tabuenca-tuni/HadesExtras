library(testthat)
library(HadesExtras)


database_settings_name <- getOption("test_database_settings_name", default = "dev_eunomia")
message("************* Testing on ", database_settings_name, " *************")

# database_settings <- helper_getDatabaseSettings()
#
# # creates the cohort tables
# checkDatabaseSettings(database_settings)


