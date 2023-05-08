options(java.parameters = "-Xmx2048m")
library(forecast)
library(xlsx)
library(readxl)
library(writexl)
library(RMySQL)
library(DBI)
library(odbc)
library(dplyr)



driver = dbDriver("MySQL")
mydb = dbConnect(driver, user = 'root', password = 'password', dbname  = 'forecasting_schema')

query = dbSendQuery(mydb, "Select * from weekly_forecast")
data = fetch(query, n=-1)

weekly_data = data.frame(data)

query = dbSendQuery(mydb, "Select * from terminal_list")
data = fetch(query, n=-1)

terminal_data = data.frame(data)

dbDisconnect(mydb)
#weekly_data = weekly_data[,c("CalendarDate","Terminal","Total.Del.Stops")]
#External Variables
amazonHistorical = readxl::read_xlsx("AmazonData.xlsx", sheet = "Historical Input")
amazonData = readxl::read_xlsx("AmazonData.xlsx", 7)
amazonDataStops = readxl::read_xlsx("AmazonData.xlsx", 8)
external_regressors = read_xlsx("ExternalVariables.xlsx", 1)
Amz_Returns=read_xlsx("Amazon_returns_MW.xlsx", 1)
#Fixing AMAZON Returns for Metro West
weekly_data[(weekly_data$Terminal==12) & ((weekly_data$Year==2022 & weekly_data$Week.Number>=15) | (weekly_data$Year==2023  & weekly_data$Week.Number<=9)),c("PCL.Del.Pcs.N")]= weekly_data[(weekly_data$Terminal==12) & ((weekly_data$Year==2022 & weekly_data$Week.Number>=15) | (weekly_data$Year==2023  & weekly_data$Week.Number<=9)),c("PCL.Del.Pcs.N")]-Amz_Returns$Pcs
list_of_terminals = sort(unique(terminal_data$Terminal),decreasing = FALSE)

for (sheet in 0:7){
  
  for (terminal in 0:(length(list_of_terminals)-1)){
    
    current_terminal = list_of_terminals[terminal+1]
    print(paste(current_terminal,sheet))
    opt_start_year = as.numeric(terminal_data[terminal_data$Terminal == current_terminal,9])
    opt_start_week = as.numeric(terminal_data[terminal_data$Terminal == current_terminal,10])
    adjustment = 0

    if (current_terminal %in% c(101,53,191,492,526)){
      opt_start_year = as.numeric(terminal_data[terminal_data$Terminal == current_terminal,9])

    }
    else if (current_terminal %in% c(480))#Owen Sound
    {opt_start_year=2021

}
    else{
      opt_start_year=2019}  
      
    
    
    
    start_year = min(as.numeric(weekly_data[weekly_data$Terminal == current_terminal,6]))
    start_week = min(as.numeric(weekly_data[weekly_data$Terminal == current_terminal & weekly_data$Year == start_year,9]))
    end_year = max(as.numeric(weekly_data[weekly_data$Terminal == current_terminal,6]))
    end_week = max(as.numeric(weekly_data[weekly_data$Terminal == current_terminal & weekly_data$Year == end_year,9]))
    
    #Change the ts values when changing actuals
    values = ts((weekly_data[weekly_data$Terminal == current_terminal,10+sheet]), start = c(start_year,start_week), end=c(end_year,end_week), frequency=52)
    #values = window(values, start = c(opt_start_year,opt_start_week))
    values = window(values, start = c(opt_start_year,opt_start_week))
    values = tsclean(values)
    
    if (sum(tail(values,6)>0.8)<3){
        values[1:length(values)]=0
      }
    length(values)
    
    current_province = terminal_data[terminal_data$Terminal == current_terminal,"Province"]
    
    current_regressors = external_regressors[external_regressors$Year >= opt_start_year,]
    current_regressors = current_regressors[current_regressors$Province == current_province,]
    current_regressors = current_regressors[(opt_start_week):nrow(current_regressors),]
    
    
    
    if (current_province == "QC"){
      holiday_names = c("Canada Day","Christmas Day","Good Friday","Labour Day","New Years Day","St Jean Baptiste Day","Thanksgiving","Truth and Reconcilliation Day","Victoria Day","Cyber-1","Cyber","Cyber+1","Cyber+2")
    }else if (current_province %in% c("AB","SK","MB")){
      holiday_names = c("Canada Day","Christmas Day","Civic Holiday","Family Day","Good Friday","Labour Day","New Years Day","Remembrance Day","Thanksgiving","Truth and Reconcilliation Day","Victoria Day","Cyber-1","Cyber","Cyber+1","Cyber+2")
    }else if (current_province %in% c("ON")){
      holiday_names = c("Canada Day","Christmas Day","Civic Holiday","Family Day","Good Friday","Labour Day","New Years Day","Thanksgiving","Truth and Reconcilliation Day","Victoria Day","Cyber-1","Cyber","Cyber+1","Cyber+2")
    }else{
      holiday_names = c("Canada Day","Christmas Day","Civic Holiday","Good Friday","Labour Day","New Years Day","Thanksgiving","Truth and Reconcilliation Day","Victoria Day","Cyber-1","Cyber","Cyber+1","Cyber+2")
    }
    
    amazonWeeklyHistorical = amazonHistorical[amazonHistorical$Terminal == current_terminal,]
    amazonWeeklyHistorical = amazonWeeklyHistorical[!is.na(amazonWeeklyHistorical$Week),]

    if (opt_start_year>2017){
      adjustment = adjustment + (opt_start_year-2017)*52
    }
    adjustment = adjustment + opt_start_week
      
    amazonWeeklyHistorical = amazonWeeklyHistorical[(adjustment):nrow(amazonWeeklyHistorical),4]#Update plus number of new weeks
    colnames(amazonWeeklyHistorical)[1] = "Amazon"
    
    if ((sheet == 0 &&  !(current_terminal %in% c(66,132,133,135,138,141,142,143,148,182,183,385,386,387,405,406,407,415,416,417,418,420,422,423,424,430,431,440,441,442,443,444,445,446,451,456,543,544))) || (sheet == 4 &&  (current_terminal %in% c(66,132,133,135,138,141,142,143,148,182,183,385,386,387,405,406,407,415,416,417,418,420,422,423,424,430,431,440,441,442,443,444,445,446,451,456,543,544)))){
      
      
      if (current_terminal %in% c(207,234,526,131,53,101)){
        amazonWeeklyExpected = amazonData[10]
        amazonWeeklyExpected = amazonWeeklyExpected[1:(52-end_week),]
        colnames(amazonWeeklyExpected)[1] = "Amazon"  
      }else{
        amazonWeeklyExpected = amazonData[as.character(current_terminal)]
        amazonWeeklyExpected = amazonWeeklyExpected[1:(52-end_week),]
        colnames(amazonWeeklyExpected)[1] = "Amazon"    
      }
      
      
      
      if (current_terminal %in% c(207,234,526,131,53,101)){
        modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52,
                    robust = FALSE, biasadj = FALSE,
                    xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names])),
                    newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names])))
      }else{
        tryCatch({
          modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52, robust = FALSE, biasadj = FALSE,
                      xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names], diff(amazonWeeklyHistorical$Amazon,1))),
                      newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names],
                                 data.frame(Amazon = 
                                              diff(c(as.numeric(amazonWeeklyHistorical[nrow(amazonWeeklyHistorical),"Amazon"]),amazonWeeklyExpected$Amazon),1)))))
        },error=function(e){
          print(paste0(current_terminal," did not use B2C regressor because of thrown error. Forecasted without B2C regressor"))
          modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52, robust = FALSE, biasadj = FALSE, xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names], diff(amazonWeeklyHistorical$Amazon,1))), newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names], data.frame(Amazon = diff(c(as.numeric(amazonWeeklyHistorical[nrow(amazonWeeklyHistorical),"Amazon"]),amazonWeeklyExpected$Amazon),1)))))
        })
        
      }
      
      fcast = modl$mean
      forecastedMean = as.numeric(fcast)
      last_value = values[length(values)]
      for (i in 1:(52-end_week)){
        forecastedMean[i] = last_value+sum(fcast[1:i])
      }
      
    }
    else if ((sheet == 1 && !(current_terminal %in% c(66,132,133,135,138,141,142,143,148,182,183,385,386,387,405,406,407,415,416,417,418,420,422,423,424,430,431,440,441,442,443,444,445,446,451,456,543,544)))){
      
      if (current_terminal %in% c(207,234,526,131,53,101)){
        amazonWeeklyExpected = amazonDataStops[10]
        amazonWeeklyExpected = amazonWeeklyExpected[1:(52-end_week),]
        colnames(amazonWeeklyExpected)[1] = "Amazon"
      }else{
        amazonWeeklyExpected = amazonDataStops[as.character(current_terminal)]
        amazonWeeklyExpected = amazonWeeklyExpected[1:(52-end_week),]
        colnames(amazonWeeklyExpected)[1] = "Amazon"  
      }
      
      
      if (current_terminal %in% c(207,234,526,131,53,101)){
        modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52, robust = FALSE, biasadj = FALSE, xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names])), newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names])))  
      }else{
        tryCatch({
          modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52, robust = FALSE, biasadj = FALSE, xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names], diff(amazonWeeklyHistorical$Amazon,1)/1.07)), newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names], data.frame(Amazon = diff(c(as.numeric(amazonWeeklyHistorical[nrow(amazonWeeklyHistorical),"Amazon"]),amazonWeeklyExpected$Amazon),1)))))    
        },error=function(e){
          print(paste0(current_terminal," did not use B2C regressor because of thrown error. Forecasted without B2C regressor"))
          modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52, robust = FALSE, biasadj = FALSE, xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names], diff(amazonWeeklyHistorical$Amazon,1)/1.07)), newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names], data.frame(Amazon = diff(c(as.numeric(amazonWeeklyHistorical[nrow(amazonWeeklyHistorical),"Amazon"]),amazonWeeklyExpected$Amazon),1)))))    
        })
        
      }
      
      fcast = modl$mean
      forecastedMean = as.numeric(fcast)
      last_value = values[length(values)]
      for (i in 1:(52-end_week)){
        forecastedMean[i] = last_value+sum(fcast[1:i])
      }
      
    }
    else if (sheet == 6){
      
      if (current_terminal %in% c(207,234,526,131,53,101)){
        amazonWeeklyExpected = amazonDataStops[10]
        amazonWeeklyExpected = amazonWeeklyExpected[1:(52-end_week),]
        colnames(amazonWeeklyExpected)[1] = "Amazon"
      }else{
        amazonWeeklyExpected = amazonDataStops[as.character(current_terminal)]
        amazonWeeklyExpected = amazonWeeklyExpected[1:(52-end_week),]
        colnames(amazonWeeklyExpected)[1] = "Amazon"  
      }
      
      
      if (current_terminal %in% c(207,234,526,131,53,101)){
        modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52, robust = FALSE, biasadj = FALSE, xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names])), newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names])))  
      }else{
        tryCatch({
          modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52, robust = FALSE, biasadj = FALSE, xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names], diff(amazonWeeklyHistorical$Amazon,1)/1.07)), newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names], data.frame(Amazon = diff(c(as.numeric(amazonWeeklyHistorical[nrow(amazonWeeklyHistorical),"Amazon"]),amazonWeeklyExpected$Amazon),1)))))    
        },error=function(e){
          print(paste0(current_terminal," did not use B2C regressor because of thrown error. Forecasted without B2C regressor"))
          modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52, robust = FALSE, biasadj = FALSE, xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names], diff(amazonWeeklyHistorical$Amazon,1)/1.07)), newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names], data.frame(Amazon = diff(c(as.numeric(amazonWeeklyHistorical[nrow(amazonWeeklyHistorical),"Amazon"]),amazonWeeklyExpected$Amazon),1)))))    
        })
        
      }
      
      fcast = modl$mean
      forecastedMean = as.numeric(fcast)
      last_value = values[length(values)]
      for (i in 1:(52-end_week)){
        forecastedMean[i] = last_value+sum(fcast[1:i])
      }
    }
    else{
      tryCatch({
        modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52, robust = FALSE, biasadj = FALSE, xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names])), newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names])))  
      },error=function(e){
        print(paste0(current_terminal," did not use B2C regressor because of thrown error. Forecasted without B2C regressor"))
        modl = stlf(y = diff(values,1), method = "arima", h = 52-end_week, s.window = 52, robust = FALSE, biasadj = FALSE, xreg = as.matrix(cbind(current_regressors[2:length(values),holiday_names])), newxreg = as.matrix(cbind(current_regressors[(length(values)+1):nrow(current_regressors),holiday_names])))
      })
      
      
      fcast = modl$mean
      forecastedMean = as.numeric(fcast)
      last_value = values[length(values)]
      for (i in 1:(52-end_week)){
        forecastedMean[i] = last_value+sum(fcast[1:i])
      }
    }
    
    
    
    #Manual Adjustment
    if (sheet %in% c(0,1,3,4,5,6) && sum(forecastedMean<0)>0){
      print(paste(current_terminal, forecastedMean),sep="---")
      forecastedMean[forecastedMean<0] =  forecastedMean[forecastedMean<0]*-1
    }

    
    current_result = data.frame(Terminal = current_terminal,
                                Year = 2023,
                                Week = seq(end_week+1,52,1),
                                Forecast  = forecastedMean)
    
    current_result[current_result$Week >= 53,2] = current_result[current_result$Week >= 53,2]+1
    current_result[current_result$Week >= 53,3] = current_result[current_result$Week >= 53,3] - 52
    
    current_result[current_result$Week >= 53,2] = current_result[current_result$Week >= 53,2]+1
    current_result[current_result$Week >= 53,3] = current_result[current_result$Week >= 53,3] - 52
    
    if (terminal == 0){
      full_result = current_result
    }
    else{
      full_result = rbind(full_result,current_result)
    }
    
  }
  write.xlsx(full_result, paste("../Output/ForecastResults_onesheet.xlsx",sep=""), sheetName = paste("Type",sheet), append = TRUE, row.names = FALSE)
  if (sheet ==0){
    full_result_new = full_result
  }else{
    full_result_new = cbind(full_result_new,full_result[,"Forecast"])
  }
}

names = c('Terminal','Year','Week.Number','PCL.Del.Pcs.N','PCL.Del.Stops.N','PCL.PU.Pcs.N','PCL.PU.Stops.N','Agent.Del.Pcs.N','Agent.PU.Pcs.N','Total.Del.Stops.N','Total.PU.Stops.N')
colnames(full_result_new) = names

ter_div = weekly_data[,c('Terminal','Division')]
ter_div = ter_div[!duplicated(ter_div),] 
full_result_new = merge(full_result_new, ter_div,on='Terminal')
full_result_new$District = NA
full_result_new$Terminal.Name = NA
full_result_new$Province = NA
full_result_new$Quarter = NA
full_result_new$Period.Number = NA
full_result_new = full_result_new[,c('Division','District','Terminal','Terminal.Name','Province','Year','Quarter','Period.Number','Week.Number','PCL.Del.Pcs.N','PCL.Del.Stops.N','PCL.PU.Pcs.N','PCL.PU.Stops.N','Agent.Del.Pcs.N','Agent.PU.Pcs.N','Total.Del.Stops.N','Total.PU.Stops.N')]

X = rbind(weekly_data,full_result_new)




write.xlsx(full_result_new, paste("../Output/ForecastResults_onesheet.xlsx",sep=""), sheetName = "Forecast", append = TRUE, row.names = FALSE)
openxlsx::write.xlsx(weekly_data, paste("../Output/Actual.xlsx",sep=""), sheetName = paste("Actual"), append = TRUE, row.names = FALSE)
openxlsx::write.xlsx(X, paste("../Output/Forecast_Actual_table.xlsx",sep=""), sheetName = "Forecast", append = TRUE, row.names = FALSE)
#openxlsx::write.xlsx(daily_data, paste("../Output//DailyForecast-Jan20_diffdist.xlsx",sep=""), append = TRUE, row.names = FALSE)
