# Setup --------------------------------------------------------------------------------------------
library(data.table)
library(lubridate)
library(jsonlite)
library(tidyr)
library(httr)

# Variables ----------------------------------------------------------------------------------------
policy_id <- "4bf184e01e0f163296ab253edd60774e2d34367d0e7b6cbc689b567d"
project <- "Pavia"
time_now <- as_datetime(now())


# Functions ----------------------------------------------------------------------------------------
extract_num <- function(x) as.numeric(gsub("[^0-9\\-]+","",as.character(x)))

loj <- function (X = NULL, Y = NULL, onCol = NULL) {
  if (truelength(X) == 0 | truelength(Y) == 0) 
    stop("setDT(X) and setDT(Y) first")
  n <- names(Y)
  X[Y, `:=`((n), mget(paste0("i.", n))), on = onCol]
}


# CNFT listings ------------------------------------------------------------------------------------
api_link_cnft <- "https://api.cnft.io/market/listings"

query <- function(page, url, project, sold) {
  httr::content(httr::POST(
    url = url, 
    body = list(
      search = "", 
      types = c("listing", "offer", "bundle"),
      project = project, 
      sort = list(`_id` = -1L), 
      priceMin = NULL, 
      priceMax = NULL, 
      page = page, 
      verified = TRUE, 
      nsfw = FALSE, 
      sold = sold, 
      smartContract = NULL
    ), 
    encode = "json"
  ), simplifyVector = TRUE)
}

query_n <- function(url, project, sold, n = "all") {
  if (n == "all") n <- query(1L, url, project, sold)[["count"]]
  out <- vector("list", n)
  for (i in seq_len(n)) {
    out[[i]] <- query(i, url, project, sold)[["results"]]
    if (length(out[[i]]) < 1L) return(out[seq_len(i - 1L)])
  }
  out
}

.CNFT <- query_n(api_link_cnft, project, sold = FALSE) |>
  lapply(data.table) |> rbindlist(fill = TRUE)

.CNFT[, link := paste0("https://cnft.io/token/", ifelse(is.na(X_id), `_id`, X_id))]

# Initialize data.table
CNFT <- data.table(asset = NA, type = NA, price = NA,
                   last_offer = NA, sc = NA, market = NA, link = NA)

for (i in 1:nrow(.CNFT)) {
  CNFT <- rbind(CNFT, data.table(asset        = .CNFT[i, assets[[1]]$metadata$name],
                                 type         = .CNFT[i, type],
                                 price        = .CNFT[i, price],
                                 last_offer   = .CNFT[i, offers],
                                 sc           = .CNFT[i, smartContractTxid],
                                 market       = "cnft.io",
                                 link         = .CNFT[i, link]))
}

CNFT <- CNFT[2:nrow(CNFT)] # Clear first row from initialization
CNFT[, price        := price/10**6]
CNFT[, sc           := ifelse(is.na(sc), "no", "yes")]

for (i in 1:nrow(CNFT)) {
  .offers <- CNFT[i, last_offer[[1]]]
  if (nrow(.offers) == 0) {
    CNFT[i, last_offer := NA]
  } else {
    CNFT[i, last_offer := max(.offers$offer/10**6)]
  }
}


# CNFT sales ---------------------------------------------------------------------------------------
.CNFTS <- query_n(api_link_cnft, project, sold = TRUE, n = 11) |>
  lapply(data.table) |> rbindlist(fill = TRUE)

.CNFTS <- .CNFTS[!is.na(soldAt)]

# Initialize data.table
CNFTS <- data.table(asset = NA, price = NA, market = NA, sold_at = NA)

for (i in 1:nrow(.CNFTS)) {
  CNFTS <- rbind(CNFTS, data.table(asset    = .CNFTS[i, assets[[1]]$metadata$name],
                                   price    = .CNFTS[i, price],
                                   market   = "cnft.io",
                                   sold_at  = .CNFTS[i, soldAt]))
}

CNFTS <- CNFTS[2:nrow(CNFTS)] # Clear first row from initialization
CNFTS[, price         := price/10**6]
CNFTS[, market        := "cnft.io"]
CNFTS[, sold_at       := as_datetime(sold_at)]
CNFTS[, sold_at_hours := difftime(time_now, sold_at, units = "hours")]
CNFTS[, sold_at_days  := difftime(time_now, sold_at, units = "days")]

CNFTS <- CNFTS[order(-sold_at), .(asset, price, sold_at, sold_at_hours, sold_at_days, market)]
CNFTS <- CNFTS[sold_at_hours <= 24*3]


# Extract information from jpg.store ---------------------------------------------------------------
JPG_list <- list()
p <- 1
while (TRUE) {
  api_link <- sprintf("https://server.jpgstoreapis.com/policy/%s/listings?page=%d", policy_id, p)
  X <- data.table(fromJSON(rawToChar(GET(api_link)$content)))
  if (nrow(X) == 0) break
  JPG_list[[p]] <- X
  p <- p + 1
}

JPG <- rbindlist(JPG_list)

JPG[, link           := paste0("https://www.jpg.store/asset/", asset_id)]
JPG[, asset          := display_name]
JPG[, price          := price_lovelace/10**6]
JPG[, sc             := "yes"]
JPG[, market         := "jpg.store"]

JPG <- JPG[, .(asset, type = "listing", price, last_offer = NA, sc, market, link)]


# JPG sales ----------------------------------------------------------------------------------------
JPGS_list <- lapply(1:7, function(p) {
  api_link <- sprintf("https://server.jpgstoreapis.com/policy/%s/sales?page=%d", policy_id, p)
  X <- data.table(fromJSON(rawToChar(GET(api_link)$content)))
  return(X)
})

JPGS <- rbindlist(JPGS_list)

JPGS[, asset          := display_name]
JPGS[, price          := price_lovelace/10**6]
JPGS[, market         := "jpg.store"]
JPGS[, sold_at        := as_datetime(confirmed_at)]
JPGS[, sold_at_hours  := difftime(time_now, sold_at, units = "hours")]
JPGS[, sold_at_days   := difftime(time_now, sold_at, units = "days")]

JPGS <- JPGS[order(-sold_at), .(asset, price, sold_at, sold_at_hours, sold_at_days, market)]
JPGS <- JPGS[sold_at_hours <= 24*3]


# Merge markets data -------------------------------------------------------------------------------
# Listings
DT <- rbindlist(list(CNFT, JPG), fill = TRUE, use.names = TRUE)

# Sales
DTS <- rbindlist(list(CNFTS, JPGS), fill = TRUE, use.names = TRUE)

# Add data collection timestamp
DT[, data_date := time_now]
DTS[, data_date := time_now]


# Save ---------------------------------------------------------------------------------------------
saveRDS(DT, file = "data/DT.rds")
saveRDS(DTS, file = "data/DTS.rds")


# Database evolution -------------------------------------------------------------------------------
DTE <- copy(DT)
if (file.exists("data/DTE_pavia.rds")) {
  cat("File data/DTE exists:", file.exists("data/DTE_pavia.rds"), "\n")
  DTE_old <- readRDS("data/DTE_pavia.rds")
  DTE <- rbindlist(list(DTE, DTE_old))
  DTE <- DTE[difftime(time_now, data_date, units = "hours") <= 24] # Only retain last 24 hours
}
saveRDS(DTE, file = "data/DTE_pavia.rds")