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




get_job_records_by_dataset_package <- function(dataPkgNumber)
{
    library(RODBC)
    con <- odbcDriverConnect("DRIVER={SQL Server};SERVER=gigasax;DATABASE=dms5;")
    strSQL = sprintf("
                SELECT *
                FROM V_Mage_Data_Package_Analysis_Jobs
                WHERE Data_Package_ID = %s",
                dataPkgNumber)
    jr <- sqlQuery(con, strSQL, stringsAsFactors=FALSE)
    close(con)            
    return(jr)
}




get_results_for_multiple_jobs = function( jobRecords ){
    toolName = unique(jobRecords[["Tool"]])
    if (length(toolName) > 1){
        stop("Contains results of more then one tool.")
    }
    library("plyr")
    results = ldply( jobRecords[["Folder"]], 
                     get_results_for_single_job, 
                     fileNamePattern=tool2suffix[[toolName]],
                    .progress = "text")
    return( results )
}




get_results_for_single_job = function(pathToFileLocation, fileNamePattern ){
    pathToFile = list.files( path=as.character(pathToFileLocation), 
                             pattern=fileNamePattern, 
                             full.names=T)
    if(length(pathToFile) == 0){
        stop("can't find the results file")
    }
    if(length(pathToFile) > 1){
        stop("ambiguous results files")
    }
    results = read.delim( pathToFile, header=T, as.is=TRUE)
    datasetName = strsplit( basename(pathToFile), split=fileNamePattern)[[1]]
    out = data.frame(DatasetName=datasetName, results, stringsAsFactors=FALSE)
    return(out)
}





