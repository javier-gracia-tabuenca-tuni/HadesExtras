

#' readDatabasesNames
#'
#' Return database names available in a databases settings yalm file
#'
#' @param path_databases_settings_yalm path to the databases settings yalm file
#'
#' @return database names
#' @export
#'
#' @importFrom checkmate assertFileExists assertList
#' @importFrom yaml read_yaml
readDatabasesNames <- function(path_databases_settings_yalm) {

  #
  # Check parameters
  #

  checkmate::assertFileExists(path_databases_settings_yalm, extension = "yml")
  databases_settings <- yaml::read_yaml(path_databases_settings_yalm)
  checkmate::assertList(databases_settings, names = "named")

  #
  # function
  #

  databases_names <- databases_settings |> names()

  return(databases_names)
}




#' Title readDatabaseSettings
#'
#' Returns the database settings for the `database_name` in the databases settings yalm file.
#' Rewrites the connectionDetails using  DatabaseConnector::createConnectionDetails
#'
#' @param path_databases_settings_yalm path to the databases settings yalm file
#' @param database_name a valid database name in the databases settings yalm file
#'
#' @return database settings
#' @export
#'
#' @importFrom checkmate assertFileExists assertList assert_subset
#' @importFrom yaml read_yaml
readDatabaseSettings <- function(path_databases_settings_yalm, database_name) {

  #
  # Check parameters
  #

  checkmate::assertFileExists(path_databases_settings_yalm, extension = "yml")
  databases_settings <- yaml::read_yaml(path_databases_settings_yalm)
  checkmate::assertList(databases_settings, names = "named")
  checkmate::assert_subset(database_name, databases_settings |> names())

  #
  # function
  #
  database_settings <- databases_settings[[database_name]]

  if(database_settings$connectionDetails$dbms == "eunomia"){
    connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  }else{
    connectionDetails <- rlang::exec(DatabaseConnector::createConnectionDetails, !!!database_settings$connectionDetails)
  }

  database_settings$connectionDetails <- connectionDetails

  # set tempEmulationSchema if in config
  if( "tempEmulationSchema" %in% names(database_settings$schemas)){
    options(sqlRenderTempEmulationSchema = database_settings$schemas$tempEmulationSchema)
  }


  # add cohort_names_table_name
  database_settings$tables$cohort_names_table <- paste0(database_settings$tables$cohort_table, "_names")


  return(database_settings)
}




#' checkDatabaseSettings
#'
#' @param database_settings list with database settings
#'
#' @return tibble with check resutls
#'
#' @importFrom DatabaseConnector createConnectionDetails connect dbGetQuery disconnect
#' @importFrom SqlRender translate
#' @importFrom dplyr add_row
#' @importFrom tibble tibble
checkDatabaseSettings <- function(database_settings) {

  #
  # Check parameters
  #

  checkmate::assertList(database_settings, names = "named")
  # TODO: check all parameters

  #
  # function
  #
  database_settings_checks <- tibble::tibble(
    step = as.character(NA),
    error = as.logical(NA),
    message = as.character(NA)
  ) |>  dplyr::filter(FALSE)


  # Check connection
  e <- tryCatch({
    connection <- DatabaseConnector::connect(database_settings$connectionDetails)
    DatabaseConnector::disconnect(connection)
  }, error=function(cond){ return(cond$message)})

  database_settings_checks <-  database_settings_checks |>
    dplyr::add_row(
      step = "Connection details",
      error = is.character(e),
      message = ifelse(is.character(e), as.character(e), paste("Connected to database",database_settings$connection$dbms ))
    )

  # set sqlRenderTempEmulationSchema if given
  if( "sqlRenderTempEmulationSchema" %in% names(database_settings)){
    options(sqlRenderTempEmulationSchema = database_settings$sqlRenderTempEmulationSchema)
    database_settings_checks <-  database_settings_checks |>
      dplyr::add_row(
        step = "sqlRenderTempEmulationSchema",
        error = FALSE,
        message = paste("Set sqlRenderTempEmulationSchema to ", database_settings$sqlRenderTempEmulationSchema)
      )
  }


  ## Check schemas
  e <- tryCatch({
    connection <- DatabaseConnector::connect(database_settings$connectionDetails)
    cdm_source <- dplyr::tbl(connection, DatabaseConnector::inDatabaseSchema(database_settings$schemas$CDM, "cdm_source")) |>
      dplyr::collect() |>
      dplyr::slice(1)
    DatabaseConnector::disconnect(connection)
  }, error=function(cond){ return(cond$message)})

  database_settings_checks <-  database_settings_checks |>
    dplyr::add_row(
      step = "CDM schema",
      error = is.character(e),
      message = ifelse(is.character(e), as.character(e), paste("CDM schema:", cdm_source$cdm_source_name, "version:", cdm_source$cdm_version ))
    )

  # Check tables
  cohort_table_created <- FALSE
  e <- tryCatch({
    cohort_table_created <- createCohortTables(database_settings)
  }, error=function(cond){ return(cond$message)})

  database_settings_checks <-  database_settings_checks |>
    dplyr::add_row(
      step = "Workbench cohort table",
      error = is.character(e),
      message = ifelse(is.character(e), as.character(e), paste(ifelse(cohort_table_created, "Workbench cohort table created", "Existing workbench cohort table loaded"), database_settings$tables$cohort_scratch))
    )


  # Check webAPI

  ## check URL
  e <- tryCatch({
    CdmSources <- ROhdsiWebApi::getCdmSources(database_settings$webApi$url)
  }, error=function(cond){ return(cond$message)})

  database_settings_checks <-  database_settings_checks |>
    dplyr::add_row(
      step = "Connection to webAPI",
      error = is.character(e),
      message = ifelse(is.character(e), as.character(e), paste("conected to webAPI", database_settings$webApi$url))
    )

  ## check sourceKey
  e <- tryCatch({
    CdmSources <- ROhdsiWebApi::getCdmSources(database_settings$webApi$url)
    if(!(database_settings$webApi$sourceKey %in% CdmSources$sourceKey)){
      stop("Invalid sourceKey")
    }
  }, error=function(cond){ return(cond$message)})

  database_settings_checks <-  database_settings_checks |>
    dplyr::add_row(
      step = "SourceKey",
      error = is.character(e),
      message = ifelse(is.character(e), as.character(e), paste("conected to webAPI's SourceKey", CdmSources$sourceKey))
    )



  return(database_settings_checks)

}





table_cohortsWorkbench_reactable <- function(cohortsSummary, deleteButtonsShinyId = NULL) {

  cohortsSummary |>  HadesExtras::assertCohortsSummary()
  deleteButtonsShinyId |> checkmate::assertString(null.ok = TRUE)


  table <- cohortsSummary  |>
    dplyr::transmute(
      database_name = database_name,
      cohortName = cohortName,
      cohort_counts_str = paste0(cohortSubjects, " (", cohortEntries, ")"),
      histogram_cohort_start_year,
      histogram_cohort_end_year,
      count_sex_str = purrr::map_chr(count_sex, .sex_tibble_to_str),
      delete_button = NA
    ) |>
    reactable::reactable(
      columns = list(
        database_name = reactable::colDef(
          name = "Database"
        ),
        cohortName = reactable::colDef(
          name = "Cohort Name"
        ),
        cohort_counts_str = reactable::colDef(
          name = "N Subjects (N Entries)"
        ),
        histogram_cohort_start_year = reactable::colDef(
          name = "Cohort Start Date",
          cell = .render_apex_plot
        ),
        histogram_cohort_end_year = reactable::colDef(
          name = "Cohort End Date",
          cell = .render_apex_plot
        ),
        count_sex_str = reactable::colDef(
          name = "Sex",
          style = function(value) {
            .bar_style(per_sex_str = value)
          },
          align = "left"
        ),
        delete_button = reactable::colDef(
          name = "",
          show = !is.null(deleteButtonsShinyId),
          sortable = FALSE,
          cell = function() htmltools::tags$button(shiny::icon("trash"))
        )
      ),
      onClick = reactable::JS(ifelse(is.null(deleteButtonsShinyId), "", paste0("
        function(rowInfo, column) {
          // Only handle click events on the 'details' column
          if (column.id !== 'delete_button') {
            return
          }

          // Display an alert dialog with details for the row
          //window.alert('Details for row ' + rowInfo.index)

          // Send the click event to Shiny, which will be available in input$show_details
          // Note that the row index starts at 0 in JavaScript, so we add 1
          if (window.Shiny) {
            Shiny.setInputValue('", deleteButtonsShinyId, "', { index: rowInfo.index + 1 }, { priority: 'event' })
          }
        }
      ")))
    )

  return(table)
}

.render_apex_plot <- function(data) {

  uiCommons <- list(
    emojis = list(
      error = "\u274c",
      warning = "\u26A0\uFE0F",
      check = "\u2705"
    ),
    colors = list(
      sexMale = "#2c5e77",
      sexFemale = "#BF616A",
      sexNa = "#8C8C8C",
      timeHist = "#00BFFF"
    )
  )

  data |>
    apexcharter::apex(apexcharter::aes(year, n ), type = "column", height = 50) |>
    apexcharter::ax_chart(sparkline = list(enabled = TRUE)) |>
    apexcharter::ax_colors(uiCommons$colors$timeHist) |>
    apexcharter::ax_yaxis(min = 0, max = max(data$n))
}

# Render a bar chart in the background of the cell
.bar_style <- function(per_sex_str){

  ## code to prepare `uiCommons` dataset goes here

  uiCommons <- list(
    emojis = list(
      error = "\u274c",
      warning = "\u26A0\uFE0F",
      check = "\u2705"
    ),
    colors = list(
      sexMale = "#2c5e77",
      sexFemale = "#BF616A",
      sexNa = "#8C8C8C",
      timeHist = "#00BFFF"
    )
  )



  height <-  "75%"
  fill_color <- uiCommons$colors$sexMale
  na_color <- uiCommons$colors$sexNa
  background_color <- uiCommons$colors$sexFemale
  text_color <-  "#FFFFFF"

  ss<-  stringr::str_split(per_sex_str, "[:blank:]")
  p_male <- ss[[1]][1] |> stringr::str_remove("%") |> as.double()
  p_na <- ss[[1]][2] |> stringr::str_remove("%") |> as.double()
  p_female <- ss[[1]][3] |> stringr::str_remove("%") |> as.double()

  list(
    backgroundImage = paste0("linear-gradient(to right, ",
                             fill_color, " ", p_male,"%, ",
                             na_color, " ", p_male,"%, ", na_color, " ", p_male+p_na,"%, ",
                             background_color ," ", p_male+p_na,"%, ",
                             background_color,
                             ")"),
    backgroundSize = paste("100%", height),
    backgroundRepeat = "no-repeat",
    backgroundPosition = "center",
    color = text_color
  )
}


.sex_tibble_to_str <- function(data){

  n_male <- data |> dplyr::filter(sex=="MALE") |> purrr::pluck("n",1, .default = 0)
  n_female <- data |> dplyr::filter(sex=="FEMALE") |> purrr::pluck("n",1, .default = 0)
  n_na <- data |> dplyr::filter( sex!="MALE" & sex!="FEMALE" ) |> purrr::pluck("n",1, .default = 0)
  n_total <- n_male + n_female + n_na

  p_male <- round(n_male/n_total*10000)/100
  p_female <- round(n_female/n_total*10000)/100
  p_na <- round(n_na/n_total*10000)/100

  return(paste0(p_male, "% ", p_na, "% ", p_female, "%" ))
}







