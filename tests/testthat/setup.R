library(testthat)
library(HadesExtras)

# settings
configurationName  <- getOption("configurationName", default = "dev_eunomia")
#configurationName  <- getOption("configurationName", default = "dev_bigquery")

#
configurations <- yaml::read_yaml(testthat::test_path("config", "test_config.yml"))

testSelectedConfiguration <- configurations[[configurationName]]
options(testSelectedConfiguration = testSelectedConfiguration)

message("************* Testing on ", configurationName, " *************")


