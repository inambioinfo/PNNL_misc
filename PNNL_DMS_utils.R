
library("RODBC")


get_server_name_for_mtdb = function( mtdbName )
# a way to find which server MT DB is located
{
    con = odbcDriverConnect("DRIVER={SQL Server};SERVER=Pogo;DATABASE=MTS_Master;")
    strSQL = sprintf("SELECT Server_Name 
                      FROM V_MTS_MT_DBs 
                      WHERE MT_DB_Name = '%s'", mtdbName)
    dbServer = sqlQuery(con,strSQL)
    close(con)
    return(as.character(dbServer[1,1]))
}







# dictionary that defines the suffix of the files given the analysis tool
tool2suffix = list("MSGFDB_DTARefinery"="_msgfdb_fht.txt",
                   "MSGFPlus_DTARefinery"="_msgfdb_fht.txt",
                   "MASIC_Finnigan"="_ReporterIons.txt")
                   


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




get_results_for_multiple_jobs = function( jobRecords){
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




get_results_for_multiple_jobs.dt = function( jobRecords){
    toolName = unique(jobRecords[["Tool"]])
    if (length(toolName) > 1){
        stop("Contains results of more then one tool.")
    }
    library("plyr")
    library("data.table")
    results = llply( jobRecords[["Folder"]], 
                     get_results_for_single_job.dt, 
                     fileNamePattern=tool2suffix[[toolName]],
                    .progress = "text")
    results.dt <- rbindlist(results)
    return( as.data.frame(results.dt) ) # in the future I may keep it as data.table
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
    results = read.delim( pathToFile, header=T, stringsAsFactors = FALSE)
    datasetName = strsplit( basename(pathToFile), split=fileNamePattern)[[1]]
    out = data.frame(DatasetName=datasetName, results, stringsAsFactors = FALSE)
    return(out)
}

library("data.table")
get_results_for_single_job.dt = function(pathToFileLocation, fileNamePattern ){
    pathToFile = list.files( path=as.character(pathToFileLocation), 
                             pattern=fileNamePattern, 
                             full.names=T)
    if(length(pathToFile) == 0){
        stop("can't find the results file")
    }
    if(length(pathToFile) > 1){
        stop("ambiguous results files")
    }
    results = read.delim( pathToFile, header=T, stringsAsFactors = FALSE)
    datasetName = strsplit( basename(pathToFile), split=fileNamePattern)[[1]]
    out = data.table(DatasetName=datasetName, results)
    return(out)
}


