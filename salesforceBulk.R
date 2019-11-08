################################################################################
# salesforceBulk.R
################################################################################
# Common file that contains functions for connectivity with the Salesforce
#   Bulk API
# Almost all functions depends on a logged-in sessionId from the
#   rforcecom.login() function.
# Eventually would like to extend the RForcecom package or fork and improve it
################################################################################

# Constants
SALESFORCE_BATCH_SIZE <- 5000
SALESFORCE_BULK_API_TIMEOUT <- 240 # seconds


rforcecom.api.getBulkEndpoint <- function(apiVersion) {
  return(paste("services/async/", apiVersion, sep = ""))
}

rforcecom.bulk.createJob <- function(session, operation, object, externalIdFieldName = NA) {
  h <- basicHeaderGatherer()
  t <- basicTextGatherer()
  endpointPath <- rforcecom.api.getBulkEndpoint(session["apiVersion"])
  URL <- paste(session["instanceURL"], endpointPath, "/job", sep = "")
  OAuthString <- paste(session["sessionID"])
  httpHeader <- c("X-SFDC-Session" = OAuthString, "Content-Type" = "application/xml")
  jobXML <- paste(
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
      <jobInfo xmlns=\"http://www.force.com/2009/06/asyncapi/dataload\">
        <operation>", operation,"</operation>
        <object>", object, "</object>",ifelse(!is.na(externalIdFieldName), paste("
        <externalIdFieldName>", externalIdFieldName, "</externalIdFieldName>
        ", sep = ""), ""),
        "<contentType>CSV</contentType>
        </jobInfo>", sep = "")
  curlPerform(url = URL, httpheader = httpHeader, headerfunction = h$update,
              writefunction = t$update, ssl.verifypeer = T, postfields = jobXML) #trying post and postfields http://curl.haxx.se/libcurl/c/curl_easy_setopt.html#CURLOPTPOSTFIELDS
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(URL)
  }
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(t$value())
  }
  x.root <- xmlRoot(xmlTreeParse(t$value(), asText = T))
  errorcode <- NA
  errormessage <- NA
  exceptioncode <- NA
  exceptionmessage <- NA
  try(errorcode <- iconv(xmlValue(x.root[["Error"]][["errorCode"]]),
                         from = "UTF-8", to = ""), TRUE)
  try(errormessage <- iconv(xmlValue(x.root[["Error"]][["message"]]),
                            from = "UTF-8", to = ""), TRUE)
  try(exceptioncode <- iconv(xmlValue(x.root[["exceptionCode"]]),
                         from = "UTF-8", to = ""), TRUE)
  try(exceptionmessage <- iconv(xmlValue(x.root[["exceptionMessage"]]),
                            from = "UTF-8", to = ""), TRUE)
  if (!is.na(errorcode) && !is.na(errormessage)) {
    stop(paste(errorcode, errormessage, sep = ": "))
  } else if (!is.na(exceptioncode) && !is.na(exceptionmessage)) {
    stop(paste(exceptioncode, exceptionmessage, sep = ": "))
  }
 return(xmlToList(x.root[["id"]]))
}

rforcecom.bulk.submitBatch <- function(session, jobId, data) {
  h <- basicHeaderGatherer()
  t <- basicTextGatherer()
  endpointPath <- rforcecom.api.getBulkEndpoint(session["apiVersion"])
  URL <- paste(session["instanceURL"], endpointPath, "/job/", jobId, "/batch", sep = "")
  OAuthString <- paste(session["sessionID"])
  httpHeader <- c("X-SFDC-Session" = OAuthString, "Content-Type" = "text/csv")
  if (is.element("data.frame", class(data))) {
    con <- textConnection("temp", "w")
    write.csv(data, file = con, quote = TRUE, eol = "\n", na = "",
              row.names = FALSE, fileEncoding = "utf8") # use "#N/A" if you want to null out a field
    postData <- paste(textConnectionValue(con), collapse = "\n")
    close(con)
  } else {
    postData <- as.character(data)
  }
  postFieldSize <- nchar(postData)
  curlPerform(url = URL, httpheader = httpHeader, headerfunction = h$update,
              writefunction = t$update, ssl.verifypeer = T,
              postfields = postData) #removing postfieldsize = postFieldSize
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(URL)
  }
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(t$value())
  }
  x.root <- xmlRoot(xmlTreeParse(t$value(), asText = T))
  errorcode <- NA
  errormessage <- NA
  exceptioncode <- NA
  exceptionmessage <- NA
  try(errorcode <- iconv(xmlValue(x.root[["Error"]][["errorCode"]]),
                         from = "UTF-8", to = ""), TRUE)
  try(errormessage <- iconv(xmlValue(x.root[["Error"]][["message"]]),
                            from = "UTF-8", to = ""), TRUE)
  try(exceptioncode <- iconv(xmlValue(x.root[["exceptionCode"]]),
                             from = "UTF-8", to = ""), TRUE)
  try(exceptionmessage <- iconv(xmlValue(x.root[["exceptionMessage"]]),
                                from = "UTF-8", to = ""), TRUE)
  if (!is.na(errorcode) && !is.na(errormessage)) {
    stop(paste(errorcode, errormessage, sep = ": "))
  } else if (!is.na(exceptioncode) && !is.na(exceptionmessage)) {
    stop(paste(exceptioncode, exceptionmessage, sep = ": "))
  }
  return(xmlToList(x.root[["id"]]))
}

rforcecom.bulk.closeJob <- function(session, jobId) {
  h <- basicHeaderGatherer()
  t <- basicTextGatherer()
  endpointPath <- rforcecom.api.getBulkEndpoint(session["apiVersion"])
  URL <- paste(session["instanceURL"], endpointPath, "/job/", jobId, sep = "")
  OAuthString <- paste(session["sessionID"])
  httpHeader <- c("X-SFDC-Session" = OAuthString, "Content-Type" = "application/xml")
  jobXML <- paste(
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
      <jobInfo xmlns=\"http://www.force.com/2009/06/asyncapi/dataload\">
        <state>Closed</state>
    </jobInfo>", sep = "")
  curlPerform(url = URL, httpheader = httpHeader, headerfunction = h$update,
              writefunction = t$update, ssl.verifypeer = T, postfields = jobXML)
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(URL)
  }
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(t$value())
  }
  x.root <- xmlRoot(xmlTreeParse(t$value(), asText = T))
  errorcode <- NA
  errormessage <- NA
  exceptioncode <- NA
  exceptionmessage <- NA
  try(errorcode <- iconv(xmlValue(x.root[["Error"]][["errorCode"]]),
                         from = "UTF-8", to = ""), TRUE)
  try(errormessage <- iconv(xmlValue(x.root[["Error"]][["message"]]),
                            from = "UTF-8", to = ""), TRUE)
  try(exceptioncode <- iconv(xmlValue(x.root[["exceptionCode"]]),
                             from = "UTF-8", to = ""), TRUE)
  try(exceptionmessage <- iconv(xmlValue(x.root[["exceptionMessage"]]),
                                from = "UTF-8", to = ""), TRUE)
  if (!is.na(errorcode) && !is.na(errormessage)) {
    stop(paste(errorcode, errormessage, sep = ": "))
  } else if (!is.na(exceptioncode) && !is.na(exceptionmessage)) {
    stop(paste(exceptioncode, exceptionmessage, sep = ": "))
  }
  return(paste(xmlToList(x.root[["id"]]), xmlToList(x.root[["state"]]), sep = ": "))
}

rforcecom.bulk.getBatchStatus <- function(session, jobId, batchId) {
  h <- basicHeaderGatherer()
  t <- basicTextGatherer()
  endpointPath <- rforcecom.api.getBulkEndpoint(session["apiVersion"])
  URL <- paste(session["instanceURL"], endpointPath, "/job/", jobId, "/batch/", batchId, sep = "")
  OAuthString <- paste(session["sessionID"])
  httpHeader <- c("X-SFDC-Session" = OAuthString)
  curlPerform(url = URL, httpheader = httpHeader, headerfunction = h$update,
              writefunction = t$update, ssl.verifypeer = T)
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(URL)
  }
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(t$value())
  }
  x.root <- xmlRoot(xmlTreeParse(t$value(), asText = T))
  errorcode <- NA
  errormessage <- NA
  exceptioncode <- NA
  exceptionmessage <- NA
  try(errorcode <- iconv(xmlValue(x.root[["Error"]][["errorCode"]]),
                         from = "UTF-8", to = ""), TRUE)
  try(errormessage <- iconv(xmlValue(x.root[["Error"]][["message"]]),
                            from = "UTF-8", to = ""), TRUE)
  try(exceptioncode <- iconv(xmlValue(x.root[["exceptionCode"]]),
                             from = "UTF-8", to = ""), TRUE)
  try(exceptionmessage <- iconv(xmlValue(x.root[["exceptionMessage"]]),
                                from = "UTF-8", to = ""), TRUE)
  if (!is.na(errorcode) && !is.na(errormessage)) {
    stop(paste(errorcode, errormessage, sep = ": "))
  } else if (!is.na(exceptioncode) && !is.na(exceptionmessage)) {
    stop(paste(exceptioncode, exceptionmessage, sep = ": "))
  }
  return(xmlToList(x.root[["state"]]))
}

rforcecom.bulk.getBatchResult <- function(session, jobId, batchId) {
  h <- basicHeaderGatherer()
  t <- basicTextGatherer()
  endpointPath <- rforcecom.api.getBulkEndpoint(session["apiVersion"])
  URL <- paste(session["instanceURL"], endpointPath, "/job/", jobId, "/batch/", batchId, "/result", sep = "")
  OAuthString <- paste(session["sessionID"])
  httpHeader <- c("X-SFDC-Session" = OAuthString)
  curlPerform(url = URL, httpheader = httpHeader, headerfunction = h$update,
              writefunction = t$update, ssl.verifypeer = T)
  return(read.csv(text = t$value(), fileEncoding = "UTF-8", encoding = "UTF-8"))
}

rforcecom.bulk.getQueryBatchResultId <- function(session, jobId, batchId) {
  h <- basicHeaderGatherer()
  t <- basicTextGatherer()
  endpointPath <- rforcecom.api.getBulkEndpoint(session["apiVersion"])
  URL <- paste(session["instanceURL"], endpointPath, "/job/", jobId, "/batch/", batchId, "/result", sep = "")
  OAuthString <- paste(session["sessionID"])
  httpHeader <- c("X-SFDC-Session" = OAuthString)
  curlPerform(url = URL, httpheader = httpHeader, headerfunction = h$update,
              writefunction = t$update, ssl.verifypeer = T)
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(URL)
  }
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(t$value())
  }
  x.root <- xmlRoot(xmlTreeParse(t$value(), asText = T))
  errorcode <- NA
  errormessage <- NA
  exceptioncode <- NA
  exceptionmessage <- NA
  try(errorcode <- iconv(xmlValue(x.root[["Error"]][["errorCode"]]),
                         from = "UTF-8", to = ""), TRUE)
  try(errormessage <- iconv(xmlValue(x.root[["Error"]][["message"]]),
                            from = "UTF-8", to = ""), TRUE)
  try(exceptioncode <- iconv(xmlValue(x.root[["exceptionCode"]]),
                             from = "UTF-8", to = ""), TRUE)
  try(exceptionmessage <- iconv(xmlValue(x.root[["exceptionMessage"]]),
                                from = "UTF-8", to = ""), TRUE)
  if (!is.na(errorcode) && !is.na(errormessage)) {
    stop(paste(errorcode, errormessage, sep = ": "))
  } else if (!is.na(exceptioncode) && !is.na(exceptionmessage)) {
    stop(paste(exceptioncode, exceptionmessage, sep = ": "))
  }
  return(xmlToList(x.root[["result"]]))
}

rforcecom.bulk.getQueryBatchResult <- function(session, jobId, batchId, resultId) {
  h <- basicHeaderGatherer()
  t <- basicTextGatherer()
  endpointPath <- rforcecom.api.getBulkEndpoint(session["apiVersion"])
  URL <- paste(session["instanceURL"], endpointPath, "/job/", jobId, "/batch/", batchId, "/result/", resultId, sep = "")
  OAuthString <- paste(session["sessionID"])
  httpHeader <- c("X-SFDC-Session" = OAuthString)
  curlPerform(url = URL, httpheader = httpHeader, headerfunction = h$update,
              writefunction = t$update, ssl.verifypeer = T)
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(URL)
  }
  if (exists("rforcecom.debug") && rforcecom.debug) {
    message(t$value())
  }
  return(read.csv(text = t$value(), fileEncoding = "UTF-8", encoding = "UTF-8"))
}

rforcecom.bulk.queryAndWait <- function(session, object, query) {
  jobId <- rforcecom.bulk.createJob(session, operation = "query", object = object)
  batchId <- rforcecom.bulk.submitBatch(session, jobId, query)
  jobStatus <- rforcecom.bulk.closeJob(session, jobId)
  batchStatus <- NA
  t1 <- proc.time()
  while(is.na(batchStatus)) {
    batchStatusTemp <- rforcecom.bulk.getBatchStatus(session, jobId, batchId)
    if (batchStatusTemp == "Failed") {
      batchStatus <- batchStatusTemp
      stop(paste("Batch ", batchId, " from job ", jobId, " ended in a 'Failed' state.", sep = ""))
    }
    if (batchStatusTemp == "Completed") {
      batchStatus <- batchStatusTemp
      message("  Success! Execution time (seconds): ", (proc.time() - t1)[["elapsed"]])
    }
    if ((proc.time() - t1)[["elapsed"]] > SALESFORCE_BULK_API_TIMEOUT) {
      stop("Query timed out.")
    }
    Sys.sleep(1)
  }
  resultId <- rforcecom.bulk.getQueryBatchResultId(session, jobId, batchId)
  return(rforcecom.bulk.getQueryBatchResult(session, jobId, batchId, resultId))
}

rforcecom.bulk.deleteAndWait <- function(session, object, data) {
  jobId <- rforcecom.bulk.createJob(session, operation = "delete", object = object)
  batchId <- rforcecom.bulk.submitBatch(session, jobId, data)
  jobStatus <- rforcecom.bulk.closeJob(session, jobId)
  batchStatus <- NA
  t1 <- proc.time()
  while(is.na(batchStatus)) {
    batchStatusTemp <- rforcecom.bulk.getBatchStatus(session, jobId, batchId)
    if (batchStatusTemp == "Failed") {
      batchStatus <- batchStatusTemp
      stop(paste("Batch ", batchId, " from job ", jobId, " ended in a 'Failed' state.", sep = ""))
    }
    if (batchStatusTemp == "Completed") {
      batchStatus <- batchStatusTemp
      message("  Success! Execution time (seconds): ", (proc.time() - t1)[["elapsed"]])
    }
    if ((proc.time() - t1)[["elapsed"]] > SALESFORCE_BULK_API_TIMEOUT) {
      stop("Query timed out.")
    }
    Sys.sleep(1)
  }
  return(rforcecom.bulk.getBatchResult(session, jobId, batchId))
}

rforcecom.bulk.pollBatchStatus <- function(session, jobId, batchId) {
  batchStatus <- NA
  t1 <- proc.time()
  while(is.na(batchStatus)) {
    batchStatusTemp <- rforcecom.bulk.getBatchStatus(session, jobId, batchId)
    if (batchStatusTemp == "Failed") {
      batchStatus <- batchStatusTemp
      stop(paste("Batch ", batchId, " from job ", jobId, " ended in a 'Failed' state.", sep = ""))
    }
    if (batchStatusTemp == "Completed") {
      batchStatus <- batchStatusTemp
      message("  Success! Execution time (seconds): ", (proc.time() - t1)[["elapsed"]])
    }
    if ((proc.time() - t1)[["elapsed"]] > SALESFORCE_BULK_API_TIMEOUT) {
      stop("Query timed out.")
    }
    Sys.sleep(1)
  }
}

rforcecom.bulk.upsertAndWait <- function(session, object, data, externalIdFieldName) {
  jobId <- rforcecom.bulk.createJob(session, operation = "upsert", object = object, externalIdFieldName = externalIdFieldName)
  message("  SFDC: JobId ", jobId)
  batches <- vector()
  numBatches <- ceiling( nrow(data) / SALESFORCE_BATCH_SIZE )
  for (i in 1:numBatches) {
    batchStart <- ( ( ( i - 1 ) * SALESFORCE_BATCH_SIZE ) + 1 )
    if (i == numBatches) {
      batchEnd <- nrow(data) # if it's the last batch, end at the last row of the data, otherwise the batch will be the max batch size
    } else {
      batchEnd <- batchStart + SALESFORCE_BATCH_SIZE - 1
    }
    message(paste("  SFDC: Upserting batch ", i, " of ", numBatches, ". Rows ", batchStart, " through ", batchEnd, ".", sep = ""))
    batches <- c( batches, rforcecom.bulk.submitBatch( session, jobId, data[ batchStart:batchEnd, ] ) ) # actual operation
    if (i == numBatches) { # if it's the last batch, then close the job
      jobStatus <- rforcecom.bulk.closeJob(session, jobId)
    }
  }
  for (b in batches) {
    message("  SFDC: Getting results from BatchId ", b)
    rforcecom.bulk.pollBatchStatus(session, jobId, b)
    if (exists("resultsTemp")) {
      resultsTemp <- rbind(resultsTemp, rforcecom.bulk.getBatchResult(session, jobId, b))
    } else {
      resultsTemp <- rforcecom.bulk.getBatchResult(session, jobId, b)
    }
  }
  return(resultsTemp)
}

# This is literally the same as bulk.upsertAndWait with a different operation
# TODO: This needs to be refactored to make a single bulk.operationAndWait
# TODO: Do the same thing for the preceding steps since they have a lot of duplicate code
rforcecom.bulk.updateAndWait <- function(session, object, data) {
  jobId <- rforcecom.bulk.createJob(session, operation = "update", object = object, externalIdFieldName = NA)
  message("  SFDC: JobId ", jobId)
  batches <- vector()
  numBatches <- ceiling( nrow(data) / SALESFORCE_BATCH_SIZE )
  for (i in 1:numBatches) {
    batchStart <- ( ( ( i - 1 ) * SALESFORCE_BATCH_SIZE ) + 1 )
    if (i == numBatches) {
      batchEnd <- nrow(data) # if it's the last batch, end at the last row of the data, otherwise the batch will be the max batch size
    } else {
      batchEnd <- batchStart + SALESFORCE_BATCH_SIZE - 1
    }
    message(paste("  SFDC: Upserting batch ", i, " of ", numBatches, ". Rows ", batchStart, " through ", batchEnd, ".", sep = ""))
    batches <- c( batches, rforcecom.bulk.submitBatch( session, jobId, data[ batchStart:batchEnd, ] ) ) # actual operation
    if (i == numBatches) { # if it's the last batch, then close the job
      jobStatus <- rforcecom.bulk.closeJob(session, jobId)
    }
  }
  for (b in batches) {
    message("  SFDC: Getting results from BatchId ", b)
    rforcecom.bulk.pollBatchStatus(session, jobId, b)
    if (exists("resultsTemp")) {
      resultsTemp <- rbind(resultsTemp, rforcecom.bulk.getBatchResult(session, jobId, b))
    } else {
      resultsTemp <- rforcecom.bulk.getBatchResult(session, jobId, b)
    }
  }
  return(resultsTemp)
}

# TODO
# rforcecom.bulk.getBatchStatusAll <- function(session, jobId)
# rforcecom.bulk.insert
# rforcecom.bulk.update
# rforcecom.bulk.upsert
# rforcecom.bulk.delete



# Create a job for each pairing of object (Lead, Contact, etc.) and operation (insert, update, etc.) and store the job ID that is returned
# http://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/asynch_api_quickstart_create_job.htm

# Break data into batches with validation for batch size limits
# https://www.salesforce.com/us/developer/docs/api_asynch/Content/asynch_api_concepts_limits.htm#batch_size_title

# Load each batch against the job ID and store the batch ID that is returned
# http://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/asynch_api_quickstart_add_batch.htm

# Repeat until all batches have been sent

# Close the job to indicate that no further batches will be sent
# http://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/asynch_api_quickstart_close_job.htm

# Check the status of all batches in the job
# http://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/asynch_api_quickstart_check_status.htm

# Do this until all batches are in a "Completed" state??
#############

# For each batch that is in a "Completed" state, there might be failed records, so retrieve the batch results
# http://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/asynch_api_quickstart_retrieve_results.htm

# Log any errors
# http://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/asynch_api_batches_failed_records.htm

# One Object at a time, each row is a record
# http://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/datafiles_csv_preparing.htm

# Break out or duplicate data based on type of load or do this on a per-job basis?
#############

# Rename columns in master data.frame object to use Salesforce API names
# https://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/datafiles_csv_rel_field_header_row.htm

# Use correct relationship field names in header
# https://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/datafiles_date_format.htm

# Convert to UTF-8
# http://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/datafiles_csv_preparing.htm

# Serialize as CSV using commas as delimiters, double-esaping double quotes, and double-quoting all values
# Use empty strings for blanks to ensure that they get updated everytime rather than ignored in the case of blanks
# http://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/datafiles_csv_valid_record_rows.htm
