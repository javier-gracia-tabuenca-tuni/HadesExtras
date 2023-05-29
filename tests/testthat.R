# This file is part of the standard setup for testthat.
# It is recommended that you do not modify it.
#
# Where should you do additional test configuration?
# Learn more about the roles of various files in:
# * https://r-pkgs.org/tests.html
# * https://testthat.r-lib.org/reference/test_package.html#special-files

library(testthat)
library(checkmate)
library(HadesExtras)

# chose database settings to tests
#options(test_database_settings_name = "dev_eunomia")
options(test_database_settings_name = "dev_bigquery")
print(getOption("test_database_settings_name"))
test_check("HadesExtras")
