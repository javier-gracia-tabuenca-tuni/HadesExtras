

tmp_inDatabaseSchema <- function (databaseSchema, table)
{
    return(dbplyr::in_schema(databaseSchema, table))
}



tmp_ConnectionHandler <- R6::R6Class(
  "tmp_ConnectionHandler",
  inherit = ResultModelManager::ConnectionHandler,
  public = list(
    #' get table
    #' @description get a dplyr table object (i.e. lazy loaded)
    #' @param table                     table name
    #' @param databaseSchema            databaseSchema to which table belongs
    tbl = function(table, databaseSchema = NULL) {
      checkmate::assertString(table)
      checkmate::assertString(databaseSchema, null.ok = TRUE)
      if (!is.null(databaseSchema)) {
        table <- dbplyr::in_schema(databaseSchema, table)
      }
      dplyr::tbl(self$getConnection(), table)
    }
  )

)


tmp_dplyr_copy_to <- function(dest, df, name = deparse(substitute(df)), overwrite = FALSE, ...) {

  if(!is.null(getOption("useBigrqueryUpload")) && getOption("useBigrqueryUpload")){

    # create empty table
    empty_df <- df |> dplyr::filter(FALSE)
    newTable <- dplyr::copy_to(dest, empty_df, name, overwrite, ...)

    # get table name as created by SqlRender
    bq_table_name <- newTable |> dbplyr::remote_name() |>
      stringr::str_replace("#", SqlRender::getTempTablePrefix()) |>
      stringr::str_to_lower()

    # get project and dataset from sqlRenderTempEmulationSchema
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
    checkmate::assertString(tempEmulationSchema)

    strings <- strsplit(tempEmulationSchema, "\\.")
    bq_project <- strings[[1]][1]
    bq_dataset <- strings[[1]][2]

    # upload
    bq_table <- bigrquery::bq_table(bq_project, bq_dataset, bq_table_name)
    bigrquery::bq_table_upload(bq_table, df)

    # tmp_table <- bigrquery::bq_table(bq_project, bq_dataset, "")
    # if(bigrquery::bq_table_exists(tmp_table)){bigrquery::bq_table_delete(tmp_table)}
    #
    #
    # bigrquery::bq_table_create(tmp_table, tibble_cohors)
    # bigrquery::bq_table_upload(tmp_table, tibble_cohors)

  }else{

    newTable <- dplyr::copy_to(dest, df, name, overwrite, ...)

  }

  return(newTable)

}
