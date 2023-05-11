

insertCohortDataToCohortTable <- function(database_settings, cohort_data){





    connection <- DatabaseConnector::connect(database_settings$connectionDetails)

    cohort_table_name <- database_settings$tables$workbench_cohort_table
    cohort_names_table_name <- paste0(cohort_table_name, "_names")

    cohort_table <- dplyr::tbl(connection, DatabaseConnector::inDatabaseSchema(database_settings$schemas$scratch, cohort_table_name))
    cohort_names_table <- dplyr::tbl(connection, DatabaseConnector::inDatabaseSchema(database_settings$schemas$scratch, cohort_names_table_name))

    person <- dplyr::tbl(connection, DatabaseConnector::inDatabaseSchema(database_settings$schemas$CDM, "person"))
    observation_period <- dplyr::tbl(connection, DatabaseConnector::inDatabaseSchema(database_settings$schemas$CDM, "observation_period"))

    cohort_data <- dplyr::copy_to(connection, cohort_data)


    # get max cohort_definition_id in cohort_table
    max_cohort_id_in_cohort_table <- cohort_table |>
        dplyr::pull(cohort_definition_id)
    max_cohort_id_in_cohort_table <- ifelse(is.na(max_cohort_id_in_cohort_table), 0, max_cohort_id_in_cohort_table)

    # append names in cohort_data to cohort_names_table and get cohort_definition_id
    dplyr::rows_append(
      cohort_names_table,
      cohort_data |>
        dplyr::distinct(cohort_name) |>
        dplyr::mutate(
          cohort_definition_id = max_cohort_id_in_cohort_table + dplyr::row_number(),
          atlas_cohort_definition_id = as.integer(NA)),
      in_place = TRUE
    )



}

