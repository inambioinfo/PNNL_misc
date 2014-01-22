# Get AScore results for a given data package
get_AScore_results <- function(dataPkgNumber)
{
    #
    library("RODBC")
    con <- odbcDriverConnect("DRIVER={SQL Server};SERVER=gigasax;DATABASE=dms5;")
    strSQL <- sprintf("SELECT *
                        FROM V_Mage_Analysis_Jobs
                        WHERE (Dataset LIKE 'DataPackage_%s%%')", dataPkgNumber)
    jobs <- sqlQuery(con, strSQL, stringsAsFactors=FALSE)
    close(con)
    #
    if(nrow(jobs) == 1){
        library("RSQLite")
        ascoreResultDB <- file.path( jobs["Folder"], "Step_5_Cyclops", "Results.db3")
        db <- dbConnect(SQLite(), dbname = ascoreResultDB)
        AScores <- dbGetQuery(db, "SELECT * FROM t_results_ascore")
        dbDisconnect(db)
        return(AScores)
    }else{
        return(NULL)
    }
}
