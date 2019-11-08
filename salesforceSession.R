################################################################################
# salesforceSession.R
################################################################################
# Common file that contains a single function to login to Salesforce via the
#   RForcecom package. The one function returns a session object containing a
#   sessionId that is needed for all Salesforce API queries.
################################################################################

# Load the required library
library(XML)
library(RCurl)

# Function to create Salesforce session object
#   http://www.salesforce.com/us/developer/docs/api_asynch/index_Left.htm#StartTopic=Content/asynch_api_quickstart_login.htm
createSalesforceSession <- function() {
  source("./common/config.R", local = TRUE)

	salesforceLogin <- function (username, password, instanceURL, apiVersion)
	{
		if (as.numeric(apiVersion) < 20)
			stop("the earliest supported API version is 20.0")
		soapBody <- paste0("<?xml version=\"1.0\" encoding=\"utf-8\" ?> \n                       <env:Envelope xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" \n                       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n                       xmlns:env=\"http://schemas.xmlsoap.org/soap/envelope/\"> \n                       <env:Body> \n                       <n1:login xmlns:n1=\"urn:partner.soap.sforce.com\">\n",
			as(newXMLNode("username", username), "character"), as(newXMLNode("password",
				password), "character"), "</n1:login> \n                       </env:Body> \n                       </env:Envelope>\n\n")
		h <- basicHeaderGatherer()
		t <- basicTextGatherer()
		URL <- paste("https://login.salesforce.com/", paste("services/Soap/u/", apiVersion, sep=""),
			sep = "")
		httpHeader <- c(SOAPAction = "login", `Content-Type` = "text/xml")
		curlPerform(url = URL, httpheader = httpHeader, postfields = soapBody,
			headerfunction = h$update, writefunction = t$update,
			ssl.verifypeer = T)
		if (exists("rforcecom.debug") && rforcecom.debug) {
			message(URL)
		}
		if (exists("rforcecom.debug") && rforcecom.debug) {
			message(t$value())
		}
		x.root <- xmlRoot(xmlTreeParse(t$value(), asText = T))
		faultstring <- NA
		try(faultstring <- iconv(xmlValue(x.root[["Body"]][["Fault"]][["faultstring"]]),
			from = "UTF-8", to = ""), TRUE)
		if (!is.na(faultstring)) {
			stop(faultstring)
		}
		sessionID <- xmlValue(x.root[["Body"]][["loginResponse"]][["result"]][["sessionId"]])
		instanceURL <- sub("(https://[^/]+/).*", "\\1", xmlValue(x.root[["Body"]][["loginResponse"]][["result"]][["serverUrl"]]))
		return(c(sessionID = sessionID, instanceURL = instanceURL,
			apiVersion = apiVersion))
	}

  return(salesforceLogin(CONFIG_SALESFORCE_USERNAME,
                         CONFIG_SALESFORCE_PW,
                         CONFIG_SALESFORCE_INSTANCE_URL,
                         CONFIG_SALESFORCE_API_VERSION))
}
