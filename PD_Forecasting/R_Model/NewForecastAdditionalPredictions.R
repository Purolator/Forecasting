options(java.parameters = "-Xmx2048m")
library(keras)
library(forecast)
library(xlsx)
library(readxl)
library(writexl)
library(openxlsx)
library(RMySQL)
library(DBI)
library(odbc)
library(caret)
library(h2o)
library(dplyr)


#Get needed data from mysql
driver = dbDriver("MySQL")
mydb = dbConnect(driver, user = 'root', password = 'password', dbname  = 'forecasting_schema')

query = dbSendQuery(mydb, "Select * from fmr where fmr.CalendarDate >= '2020-04-12'")
data = fetch(query, n=-1)

daily_data = data.frame(data)

query = dbSendQuery(mydb, "Select * from daily_forecast")
data = fetch(query, n=-1)

daily_forecast = data.frame(data)

query = dbSendQuery(mydb, "Select * from holidays_by_day WHERE holidays_by_day.Year >= 2019")
data = fetch(query, n=-1)

day_information = data.frame(data)

dbDisconnect(mydb)

daily_data = daily_data %>% group_by(Period.Number, Week.Day,Terminal) %>% mutate(Couriers.Day.Period.Average = mean(Couriers.Day)) %>% ungroup()
daily_data = daily_data %>% group_by(Period.Number, Week.Day,Terminal) %>% mutate(Courier.AM.Hours.Period.Average = mean(Courier.AM.Hours)) %>% ungroup()
daily_data = daily_data %>% group_by(Period.Number, Week.Day,Terminal) %>% mutate(Courier.Delivery.Hours.Period.Average = mean(Courier.Delivery.Hours)) %>% ungroup()
daily_data = daily_data %>% group_by(Period.Number, Week.Day,Terminal) %>% mutate(Courier.Pickup.Hours.Period.Average = mean(Courier.Pickup.Hours)) %>% ungroup()
daily_data = daily_data %>% group_by(Period.Number, Week.Day,Terminal) %>% mutate(Courier.PM.Hours.Period.Average = mean(Courier.PM.Hours)) %>% ungroup()
daily_data = daily_data %>% group_by(Period.Number, Week.Day,Terminal) %>% mutate(Courier.Other.Hours.Period.Average = mean(Courier.Other.Hours)) %>% ungroup()
daily_data = daily_data %>% group_by(Period.Number, Week.Day,Terminal) %>% mutate(AM.Dock.Hours.Period.Average = mean(AM.Dock.Hours)) %>% ungroup()
daily_data = daily_data %>% group_by(Period.Number, Week.Day,Terminal) %>% mutate(PM.Dock.Hours.Period.Average = mean(PM.Dock.Hours)) %>% ungroup()

average_couriers_per_day = read_xlsx("AverageCouriersPerDay.xlsx", sheet = "Avg Couriers")
average_couriers_per_day = average_couriers_per_day[average_couriers_per_day$Date >= as.Date("2020-04-12"),]
#First Part - Run to here
h2o.init()
#average_couriers_per_day = average_couriers_per_day[average_couriers_per_day$Date >= as.Date("2020-05-17"),]

#Training Process -------------

trainingData = as.h2o(daily_data[,c(3,7,8,9,10,11,12,13,14,31,37,43,44,45,46,47,48,49,50,15,16,17,19,20,21,23,24)])

#Starts h2o and trains models (don't need to run this every time, maybe twice a year)
h2o.init(nthreads = -1,
         max_mem_size = "4G")

splits = h2o.splitFrame(data = trainingData, ratios = c(0.7, 0.15), seed = 1)

train <- splits[[1]]
valid <- splits[[2]]
test <- splits[[3]]
# Training Part
couriers_day_model = h2o.automl(x = c("Terminal","PCL.Del.Pcs","PCL.Del.Stops","PCL.PU.Pcs","PCL.PU.Stops","Agent.Del.Pcs","Total.Del.Stops","Agent.PU.Pcs","Total.PU.Stops","Week.Number","Week.Day","Couriers.Day.Period.Average"), y = c("Couriers.Day"), training_frame = train, max_models = 15)
h2o.saveModel(couriers_day_model@leader, path = "C:\\Forecast tools\\P&D Forecast\\P&D Forecast v2\\ML Trained Models")
courier_AM_hours_model = h2o.automl(x = c("Terminal","PCL.Del.Pcs","PCL.Del.Stops","PCL.PU.Pcs","PCL.PU.Stops","Agent.Del.Pcs","Total.Del.Stops","Agent.PU.Pcs","Total.PU.Stops","Week.Number","Week.Day"), y = c("Courier.AM.Hours"), training_frame = train, max_models = 15)
h2o.saveModel(courier_AM_hours_model@leader, path = "C:\\Forecast tools\\P&D Forecast\\P&D Forecast v2\\ML Trained Models")
courier_delivery_hours_model = h2o.automl(x = c("Terminal","PCL.Del.Pcs","PCL.Del.Stops","PCL.PU.Pcs","PCL.PU.Stops","Agent.Del.Pcs","Total.Del.Stops","Agent.PU.Pcs","Total.PU.Stops","Week.Number","Week.Day"), y = "Courier.Delivery.Hours", training_frame = train, max_models = 15)
h2o.saveModel(courier_delivery_hours_model@leader, path = "C:\\Forecast tools\\P&D Forecast\\P&D Forecast v2\\ML Trained Models")
courier_pickup_hours_model = h2o.automl(x = c("Terminal","PCL.Del.Pcs","PCL.Del.Stops","PCL.PU.Pcs","PCL.PU.Stops","Agent.Del.Pcs","Total.Del.Stops","Agent.PU.Pcs","Total.PU.Stops","Week.Number","Week.Day"), y = "Courier.Pickup.Hours", training_frame = train, max_models = 15)
h2o.saveModel(courier_pickup_hours_model@leader, path = "C:\\Forecast tools\\P&D Forecast\\P&D Forecast v2\\ML Trained Models")
courier_PM_hours_model = h2o.automl(x = c("Terminal","PCL.Del.Pcs","PCL.Del.Stops","PCL.PU.Pcs","PCL.PU.Stops","Agent.Del.Pcs","Total.Del.Stops","Agent.PU.Pcs","Total.PU.Stops","Week.Number","Week.Day"), y = "Courier.PM.Hours", training_frame = train, max_models = 15)
h2o.saveModel(courier_PM_hours_model@leader, path = "C:\\Forecast tools\\P&D Forecast\\P&D Forecast v2\\ML Trained Models")
courier_other_hours_model = h2o.automl(x = c("Terminal","PCL.Del.Pcs","PCL.Del.Stops","PCL.PU.Pcs","PCL.PU.Stops","Agent.Del.Pcs","Total.Del.Stops","Agent.PU.Pcs","Total.PU.Stops","Week.Number","Week.Day"), y = "Courier.Other.Hours", training_frame = train, max_models = 15)
h2o.saveModel(courier_other_hours_model@leader, path = "C:\\Forecast tools\\P&D Forecast\\P&D Forecast v2\\ML Trained Models")
am_dock_hours_model = h2o.automl(x = c("Terminal","PCL.Del.Pcs","PCL.Del.Stops","PCL.PU.Pcs","PCL.PU.Stops","Agent.Del.Pcs","Total.Del.Stops","Agent.PU.Pcs","Total.PU.Stops","Week.Number","Week.Day"), y = "AM.Dock.Hours", training_frame = train, max_models = 15)
h2o.saveModel(am_dock_hours_model@leader, path = "C:\\Forecast tools\\P&D Forecast\\P&D Forecast v2\\ML Trained Models")
pm_dock_hours_model = h2o.automl(x = c("Terminal","PCL.Del.Pcs","PCL.Del.Stops","PCL.PU.Pcs","PCL.PU.Stops","Agent.Del.Pcs","Total.Del.Stops","Agent.PU.Pcs","Total.PU.Stops","Week.Number","Week.Day"), y = "PM.Dock.Hours", training_frame = train, max_models = 15)
h2o.saveModel(pm_dock_hours_model@leader, path = "C:\\Forecast tools\\P&D Forecast\\P&D Forecast v2\\ML Trained Models")

# Part TWO - Run to here - Do this training part every 6 months

#Models are now trained 
#Predicting with the models ---------------- C:/Forecast tools/P&D Forecast/P&D Forecast v2
#Need daily_forecast for forecasted pcs/stops, day_information to get the week number and weekday number for the date
#Loading the trained Models

couriers_day_model = h2o.loadModel("../../../6- First P&D Forecast Run-Jan 2023/P&D Forecast - Ramy/ML Trained Models/StackedEnsemble_AllModels_AutoML_20220530_195346")
courier_AM_hours_model = h2o.loadModel("../../../6- First P&D Forecast Run-Jan 2023/P&D Forecast - Ramy/ML Trained Models/StackedEnsemble_AllModels_AutoML_20220530_203218")
courier_delivery_hours_model = h2o.loadModel("../../../6- First P&D Forecast Run-Jan 2023/P&D Forecast - Ramy/ML Trained Models/StackedEnsemble_AllModels_AutoML_20220530_213945")
courier_pickup_hours_model = h2o.loadModel("../../../6- First P&D Forecast Run-Jan 2023/P&D Forecast - Ramy/ML Trained Models/StackedEnsemble_AllModels_AutoML_20220530_222534")
courier_PM_hours_model = h2o.loadModel("../../../6- First P&D Forecast Run-Jan 2023/P&D Forecast - Ramy/ML Trained Models/StackedEnsemble_AllModels_AutoML_20220530_231528")
courier_other_hours_model = h2o.loadModel("../../../6- First P&D Forecast Run-Jan 2023/P&D Forecast - Ramy/ML Trained Models/StackedEnsemble_AllModels_AutoML_20220530_234605")
am_dock_hours_model = h2o.loadModel("../../../6- First P&D Forecast Run-Jan 2023/P&D Forecast - Ramy/ML Trained Models/StackedEnsemble_AllModels_AutoML_20220531_001207")
pm_dock_hours_model = h2o.loadModel("../../../6- First P&D Forecast Run-Jan 2023/P&D Forecast - Ramy/ML Trained Models/StackedEnsemble_AllModels_AutoML_20220531_005703")
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          

#Getting week day
daily_forecast$Week.Day = 0
daily_forecast$Week.Number = 0
for (count in 1:length(daily_forecast[,1])){
  daily_forecast[count,14] = unique(day_information[day_information$Date == daily_forecast[count,1],6])
  daily_forecast[count,15] = unique(day_information[day_information$Date == daily_forecast[count,1],4])
}


couriers_per_day = h2o.predict(couriers_day_model, as.h2o(cbind(daily_forecast[,c(2,6,7,8,9,10,12,11,13,15,14)], data.frame(Couriers.Day.Period.Average = average_couriers_per_day$Avg) )))
couriers_per_day = as.data.frame(couriers_per_day)

courier_AM_hours = h2o.predict(courier_AM_hours_model, as.h2o(daily_forecast[,c(2,6,7,8,9,10,12,11,13,15,14)]))
courier_AM_hours = as.data.frame(courier_AM_hours)

courier_delivery_hours = h2o.predict(courier_delivery_hours_model, as.h2o(daily_forecast[,c(2,6,7,8,9,10,12,11,13,15,14)]))
courier_delivery_hours = as.data.frame(courier_delivery_hours)

courier_pickup_hours = h2o.predict(courier_pickup_hours_model, as.h2o(daily_forecast[,c(2,6,7,8,9,10,12,11,13,15,14)]))
courier_pickup_hours = as.data.frame(courier_pickup_hours)

courier_PM_hours = h2o.predict(courier_PM_hours_model, as.h2o(daily_forecast[,c(2,6,7,8,9,10,12,11,13,15,14)]))
courier_PM_hours = as.data.frame(courier_PM_hours)

courier_other_hours = h2o.predict(courier_other_hours_model, as.h2o(daily_forecast[,c(2,6,7,8,9,10,12,11,13,15,14)]))
courier_other_hours = as.data.frame(courier_other_hours)

am_dock_hours = h2o.predict(am_dock_hours_model, as.h2o(daily_forecast[,c(2,6,7,8,9,10,12,11,13,15,14)]))
am_dock_hours = as.data.frame(am_dock_hours)

pm_dock_hours = h2o.predict(pm_dock_hours_model, as.h2o(daily_forecast[,c(2,6,7,8,9,10,12,11,13,15,14)]))
pm_dock_hours = as.data.frame(pm_dock_hours)


daily_forecast = cbind(daily_forecast, Couriers_per_Day = round(couriers_per_day),courier_AM_hours=round(courier_AM_hours,2),courier_delivery_hours=round(courier_delivery_hours),courier_pickup_hours=round(courier_pickup_hours),courier_PM_hours=round(courier_PM_hours,2),courier_other_hours=round(courier_other_hours),am_dock_hours=round(am_dock_hours),pm_dock_hours=round(pm_dock_hours))
colnames(daily_forecast)[16:23] = c("Couriers per Day","Courier AM Hours", "Courier Delivery Hours", "Courier Pickup Hours", "Courier PM Hours", "Courier Other Hours", "AM Dock Hours", "PM Dock Hours")
daily_forecast[daily_forecast<0] = 0
daily_forecast["Courier Del+PU Hours"] = daily_forecast["Courier Delivery Hours"]+daily_forecast["Courier Pickup Hours"]
daily_forecast = daily_forecast[,c(names(daily_forecast)[1:19],"Courier Del+PU Hours",names(daily_forecast)[20:23])]
#daily_forecast$Calendar.Date = 
daily_forecast$Total.Del.Pcs = daily_forecast$PCL.Del.Pcs + daily_forecast$Agent.Del.Pcs
daily_forecast['Total PU Pcs']=daily_forecast$PCL.PU.Pcs + daily_forecast$Agent.PU.Pcs
daily_forecast$Code = paste(daily_forecast$Terminal,as.numeric(as.Date(daily_forecast$Calendar.Date) -as.Date(0, origin="1899-12-30", tz='UTC')),sep="-")
daily_forecast = daily_forecast[,c(names(daily_forecast)[1:2],"Code",names(daily_forecast)[3:19],"Courier Del+PU Hours",names(daily_forecast)[21:26])]




#daily_forecast$Couriers_Day = round(couriers_per_day)
#daily_forecast$Courier_AM_Hours = round(courier_AM_hours)
#daily_forecast$Courier_Delivery_Hours = round(courier_delivery_hours)
#daily_forecast$Courier_Pickup_Hours = round(courier_pickup_hours)
#daily_forecast$Courier_PM_Hours = round(courier_PM_hours)
#daily_forecast$Courier_Other_Hours = round(courier_other_hours)
#daily_forecast$AM_Dock_Hours = round(am_dock_hours)
#daily_forecast$PM_Dock_Hours = round(pm_dock_hours)


openxlsx::write.xlsx(daily_forecast, "../Output/Daily_Forecast_Extended - Apr 28th.xlsx")

