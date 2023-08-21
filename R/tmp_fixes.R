
#' tmp fix for DatabaseConnector::inDatabaseSchema
#'
#' till this is fixed https://github.com/OHDSI/DatabaseConnector/issues/236
#'
#' @param databaseSchema The name of the database schema.
#' @param table The name of the table.
#'
#' @importFrom dbplyr in_schema
#'
#' @return The fully qualified table name with the database schema.
#'
#' @export
tmp_inDatabaseSchema <- function (databaseSchema, table)
{
    return(dbplyr::in_schema(databaseSchema, table))
}


#' tmp fix for ConnectionHandler
#'
#' till this is fixed https://github.com/OHDSI/DatabaseConnector/issues/236
#'
#' @importFrom R6 R6Class
#' @importFrom ResultModelManager ConnectionHandler
#' @importFrom checkmate assertString
#' @importFrom dbplyr in_schema
#' @importFrom dplyr tbl
#'
#' @export

tmp_ConnectionHandler <- R6::R6Class(
  "tmp_ConnectionHandler",
  inherit = ResultModelManager::ConnectionHandler,
  public = list(
    #'
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

#' tmp fix for dplyr::copy_to
#'
#' dplyr::copy_to is very slow to upload tables to BQ.
#' This is function calls dplyr::copy_to except if option "useBigrqueryUpload" option is set to TRUE
#' In that case it uses package bigrquery to upload the table
#'
#' @param dest The destination database connection or table name.
#' @param df The data frame to be copied.
#' @param name The name of the destination table (default is the name of the data frame).
#' @param overwrite Logical value indicating whether to overwrite an existing table (default is FALSE).
#' @param ... pass parameters to dplyr::copy_to
#'
#' @importFrom dplyr filter copy_to
#' @importFrom dbplyr remote_name
#' @importFrom stringr str_replace str_to_lower
#' @importFrom checkmate assertString
#' @importFrom bigrquery bq_table bq_table_upload
#'
#' @return The new table created or the updated table.
#'
#' @export
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
