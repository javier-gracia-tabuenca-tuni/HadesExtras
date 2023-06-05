

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
