# settings
configurationName  <- getOption("configurationName", default = "dev_eunomia")
configurationName  <- getOption("configurationName", default = "dev_bigquery")
#
configurations <- yaml::read_yaml(testthat::test_path("config", "test_config.yml"))

testSelectedConfiguration <- configurations[[configurationName]]

message("************* Testing on ", configurationName, " *************")


