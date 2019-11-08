# R-salesforce-bulk
Libraries that simplify importing/exporting data between R and Salesforce via the Salesforce Bulk API.

# Samples
accounts_query <- paste("SELECT Name FROM Account", sep = "")

accounts <- rforcecom.bulk.queryAndWait(session, object = "Org__c", query = accounts_query/)

