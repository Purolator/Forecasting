options(java.parameters = "-Xmx2048m")
library(shiny)
library(plotly)
library(shinydashboard)
library(RMySQL)
library(DBI)
library(odbc)
library(dplyr)

driver = dbDriver("MySQL")
mydb = dbConnect(driver, user = 'root', password = 'password', dbname  = 'forecasting_schema')

query = dbSendQuery(mydb, "Select * from daily_forecast_accuracy where daily_forecast_accuracy.`Calendar Date` >= '2019-01-01' AND !(daily_forecast_accuracy.`Calendar Date` = '2019-05-20') AND 
                    !(daily_forecast_accuracy.`Calendar Date` = '2019-07-01') AND !(daily_forecast_accuracy.`Calendar Date` = '2019-08-05') AND !(daily_forecast_accuracy.`Calendar Date` = '2019-09-02') AND !(daily_forecast_accuracy.`Calendar Date` = '2019-10-14') AND !(daily_forecast_accuracy.`Calendar Date` = '2019-11-11') AND !(daily_forecast_accuracy.`Calendar Date` = '2019-12-25') AND !(daily_forecast_accuracy.`Calendar Date` = '2019-12-26') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-01-01') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-02-17') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-04-10') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-05-18') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-07-01') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-06-24') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-08-03') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-09-07') 
                    AND !(daily_forecast_accuracy.`Calendar Date` = '2020-10-12') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-11-11') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-12-25') AND !(daily_forecast_accuracy.`Calendar Date` = '2020-12-28') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-01-01') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-02-15') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-04-02') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-05-24') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-07-01') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-06-24') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-08-02') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-09-06') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-09-30') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-10-11') AND !(daily_forecast_accuracy.`Calendar Date` = '2021-12-27') 
                    AND !(daily_forecast_accuracy.`Calendar Date` = '2021-12-28') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-01-03') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-04-15') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-04-15') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-05-23') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-06-24') 
                    AND !(daily_forecast_accuracy.`Calendar Date` = '2022-07-01') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-08-01') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-09-05') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-09-30') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-10-10') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-12-26') AND !(daily_forecast_accuracy.`Calendar Date` = '2022-12-27') AND !(daily_forecast_accuracy.`Calendar Date` = '2023-01-02')")
data = fetch(query, n=-1)

accuracy_data = data.frame(data)

query = dbSendQuery(mydb, "Select * from terminal_list")
data = fetch(query, n=-1)

terminal_data = data.frame(data)

dbDisconnect(mydb)

#terminal_data = terminal_data[!(terminal_data$Terminal %in% c(207,234,526)),]

accuracy_data$Calendar.Date = as.Date(accuracy_data$Calendar.Date)
accuracy_data$Total.Del.Pcs.F = accuracy_data$PCL.Del.Pcs.F+accuracy_data$Agent.Del.Pcs.F
accuracy_data$Total.Del.Pcs.A = accuracy_data$PCL.Del.Pcs.A+accuracy_data$Agent.Del.Pcs.A
accuracy_data = merge(x=accuracy_data, y=data.frame(terminal_data[,c(2,3)]), by = c("Terminal"), all=TRUE)
accuracy_data$Total.PU.Pcs.F = accuracy_data$PCL.PU.Pcs.F+accuracy_data$Agent.PU.Pcs.F
accuracy_data$Total.PU.Pcs.A = accuracy_data$PCL.PU.Pcs.A+accuracy_data$Agent.PU.Pcs.A
accuracy_data$Total.Pcs.F = accuracy_data$Total.Del.Pcs.F+accuracy_data$Total.PU.Pcs.F
accuracy_data$Total.Pcs.A = accuracy_data$Total.Del.Pcs.A+accuracy_data$Total.PU.Pcs.A

ui <- dashboardPage(
  
  dashboardHeader(title = "Accuracy Dashboard"),
  dashboardSidebar(sidebarMenu(id = "tab",
    menuItem("National", tabName = "National", icon = icon("globe-americas")),
    menuItem("Divisional", tabName = "Divisional", icon = icon("flag")),
    menuItem("Terminal", tabName = "Terminal", icon = icon("warehouse"))
  )),
  dashboardBody(
    tabItems(
      tabItem(tabName = "National",
              fluidRow(
                column(4,dateInput(inputId = "start_date_national", label = "Start Date", min = "2019-01-01", max= max(accuracy_data$Calendar.Date), value = "2019-01-01")),
                column(4,dateInput(inputId = "end_date_national", label = "End Date",min = "2019-01-01", max = max(accuracy_data$Calendar.Date), value = max(accuracy_data$Calendar.Date))),
                column(4,selectInput(inputId = "commodity_type_national", label = "Select which element to evaluate", choices = c("Total Del Pcs", "Total Del Stops", "Total Pickup Pcs", "Total Pcs")))
              ),
              fluidRow(
                checkboxInput(inputId = "weekends_national", label = "Include weekends?")
              ),
              fluidRow(
                plotlyOutput("national_graph")
              ),
              fluidRow(
                valueBoxOutput("national_accuracy"),
                valueBoxOutput("national_st_dev"),
                valueBoxOutput("national_percentage_over_under")
              )
              ),
      tabItem(tabName = "Divisional",
              fluidRow(
                column(3,selectInput(inputId = "division", label = "Select Division", choices = unique(as.character(accuracy_data$Division)))),
                column(3,dateInput(inputId = "start_date_divisional", label = "Start Date", min = "2019-01-01", max= max(accuracy_data$Calendar.Date), value = "2019-01-01")),
                column(3,dateInput(inputId = "end_date_divisional", label = "End Date",min = "2019-01-01", max = max(accuracy_data$Calendar.Date), value = max(accuracy_data$Calendar.Date))),
                column(3,selectInput(inputId = "commodity_type_divisional", label = "Select which element to evaluate", choices = c("Total Del Pcs", "Total Del Stops")))
              ),
              fluidRow(
                checkboxInput(inputId = "weekends_divisional", label = "Include weekends?")
              ),
              fluidRow(
                plotlyOutput("divisional_graph")
              ),
              fluidRow(
                valueBoxOutput("divisional_accuracy"),
                valueBoxOutput("divisional_st_dev"),
                valueBoxOutput("divisional_percentage_over_under")
              )),
      tabItem(tabName = "Terminal",
              fluidRow(
                column(3,selectInput(inputId = "terminal", label = "Select Terminal", choices = unique(as.character(accuracy_data$Terminal)))),
                column(3,dateInput(inputId = "start_date_terminal", label = "Start Date", min = "2019-01-01", max= max(accuracy_data$Calendar.Date), value = "2019-01-01")),
                column(3,dateInput(inputId = "end_date_terminal", label = "End Date",min = "2019-01-01", max = max(accuracy_data$Calendar.Date), value = max(accuracy_data$Calendar.Date))),
                column(3,selectInput(inputId = "commodity_type_terminal", label = "Select which element to evaluate", choices = c("Total Del Pcs", "Total Del Stops")))
              ),
              fluidRow(
                checkboxInput(inputId = "weekends_terminal", label = "Include weekends?")
              ),
              fluidRow(
                plotlyOutput("terminal_graph")
              ),
              fluidRow(
                valueBoxOutput("terminal_accuracy"),
                valueBoxOutput("terminal_st_dev"),
                valueBoxOutput("terminal_percentage_over_under")
              ),
              fluidRow(
                dataTableOutput("terminal_accuracy_rankings")
              ))
    )
  )
  
  
)

server <- function(input, output, session) {
  
  graph_data = reactive({
    
    if (input$tab == "National"){
      useful_df = accuracy_data[accuracy_data$Calendar.Date >= input$start_date_national & accuracy_data$Calendar.Date <= input$end_date_national,]
      if (!input$weekends_national){
        useful_df = useful_df[useful_df$Week.Day %in% c(2,3,4,5,6),]
      }
      if (input$commodity_type_national == "Total Del Pcs"){
        useful_df = useful_df[,c(2,28,29)]
      }else if (input$commodity_type_national == "Total Pickup Pcs"){
        useful_df = useful_df[,c(2,31,32)] 
      }else if (input$commodity_type_national == "Total Pcs"){
        useful_df = useful_df[,c(2,33,34)] 
      }else{
        useful_df = useful_df[,c(2,23,22)]
      }
      useful_df = useful_df[,c(2,3)] %>% group_by(useful_df$Calendar.Date) %>% summarise_each(funs(sum))
    }
    else if (input$tab == "Divisional"){
      useful_df = accuracy_data[accuracy_data$Calendar.Date >= input$start_date_divisional & accuracy_data$Calendar.Date <= input$end_date_divisional,]
      if (!input$weekends_divisional){
        useful_df = useful_df[useful_df$Week.Day %in% c(2,3,4,5,6),]
      }
      if (input$commodity_type_divisional == "Total Del Pcs"){
        useful_df = useful_df[,c(2,28,29,30)]
      }else{
        useful_df = useful_df[,c(2,23,22,30)]
      }
      useful_df = useful_df[,c(2,3)] %>% group_by(useful_df$Calendar.Date, useful_df$Division) %>% summarise_each(funs(sum))
      colnames(useful_df)[2] = "Division"
      useful_df = useful_df[useful_df$Division == input$division,c(1,3,4,2)]
    }
    else if (input$tab == "Terminal"){
      useful_df = accuracy_data[accuracy_data$Calendar.Date >= input$start_date_terminal & accuracy_data$Calendar.Date <= input$end_date_terminal,]
      if (!input$weekends_terminal){
        useful_df = useful_df[useful_df$Week.Day %in% c(2,3,4,5,6),]
      }
      if (input$commodity_type_terminal == "Total Del Pcs"){
        useful_df = useful_df[,c(2,28,29,1)]
      }else{
        useful_df = useful_df[,c(2,23,22,1)]
      }
      useful_df = useful_df[,c(2,3)] %>% group_by(useful_df$Calendar.Date, useful_df$Terminal) %>% summarise_each(funs(sum))
      colnames(useful_df)[2] = "Terminal"
      useful_df = useful_df[useful_df$Terminal == input$terminal,c(1,3,4,2)]
    }
    colnames(useful_df)[1] = "Date"
    as.data.frame(useful_df)
  })
  
  
  terminal_table_data = reactive({
    useful_df = accuracy_data[accuracy_data$Calendar.Date >= input$start_date_terminal & accuracy_data$Calendar.Date <= input$end_date_terminal,]
    if (!input$weekends_terminal){
      useful_df = useful_df[useful_df$Week.Day %in% c(2,3,4,5,6),]
    }
    if (input$commodity_type_terminal == "Total Del Pcs"){
      useful_df = useful_df[,c(2,28,29,1)]
    }else{
      useful_df = useful_df[,c(2,23,22,1)]
    }
    
    useful_df$Accuracy = 1- abs(useful_df[,2]-useful_df[,3])/useful_df[,3]
    useful_df[is.infinite(useful_df$Accuracy),c("Accuracy")] = 0
    useful_df[useful_df<0] = 0
    useful_df$AbsoluteDifference = abs(useful_df[,3]-useful_df[,2])
    list_of_terminals = unique(useful_df$Terminal)
    
    accuracy_list = data.frame(Terminal = 0, Accuracy = 0, AverageAbsoluteDifference = 0)
    for (term in 1:length(list_of_terminals)){
      current_terminal = list_of_terminals[term]
      current_accuracy = mean(useful_df[useful_df$Terminal == current_terminal,c("Accuracy")], na.rm = TRUE)
      current_difference = mean(useful_df[useful_df$Terminal == current_terminal,c("AbsoluteDifference")], na.rm = TRUE)
      accuracy_list = rbind(accuracy_list, data.frame(Terminal = current_terminal, Accuracy = round(current_accuracy,4)*100, AverageAbsoluteDifference = round(current_difference,2)))
    }
    accuracy_list = accuracy_list[-1,]
    accuracy_list
  })
  
  accuracy = reactive({
    df = graph_data()
    df = df[!(df$Date %in% c(as.Date("2019-01-01"),as.Date("2019-04-19"))),]
    df$Accuracy = 1- abs(df[,2]-df[,3])/df[,3]
    df[is.infinite(df$Accuracy),c("Accuracy")] = 0
    df[df<0] = 0
    mean(df$Accuracy, na.rm = TRUE)
  })
  
  st_dev = reactive({
    df = graph_data()
    df = df[!(df$Date %in% c(as.Date("2019-01-01"),as.Date("2019-04-19"))),]
    df$Accuracy = 1- abs(df[,2]-df[,3])/df[,3]
    df[is.infinite(df$Accuracy),c("Accuracy")] = 0
    sd(df$Accuracy, na.rm = TRUE)
  })
  
  percentage_over_under = reactive({
    df = graph_data()
    df = df[!(df$Date %in% c(as.Date("2019-01-01"),as.Date("2019-04-19"))),]
    df$Accuracy = (df[,2]-df[,3])/df[,3]
    df[is.infinite(df$Accuracy),c("Accuracy")] = 0
    mean(df$Accuracy, na.rm = TRUE)
  })
  
  output$national_graph = renderPlotly({
    df = graph_data()
    colnames(df)[2] = "Forecast"
    colnames(df)[3] = "Actual"
    g = ggplot(df, aes(Date))+geom_line(aes(y=Forecast, colour = "Forecast"))+geom_line(aes(y=Actual, colour="Actual"))
    ggplotly(g)
  })
  
  output$national_accuracy = renderValueBox({
    valueBox(value = round(accuracy(),4)*100, subtitle = "Accuracy")
  })
  
  output$national_st_dev = renderValueBox({
    valueBox(value = round(st_dev(),4)*100, subtitle = "Standard Deviation")
  })
  
  output$national_percentage_over_under = renderValueBox({
    valueBox(value = round(percentage_over_under(),4)*100, subtitle = "Over/Under Forecasted")
  })
  
  output$divisional_graph = renderPlotly({
    df = graph_data()
    colnames(df)[2] = "Forecast"
    colnames(df)[3] = "Actual"
    g = ggplot(df, aes(Date))+geom_line(aes(y=Forecast, colour = "Forecast"))+geom_line(aes(y=Actual, colour="Actual"))
    ggplotly(g)
  })
  
  output$divisional_accuracy = renderValueBox({
    valueBox(value = round(accuracy(),4)*100, subtitle = "Accuracy")
  })
  
  output$divisional_st_dev = renderValueBox({
    valueBox(value = round(st_dev(),4)*100, subtitle = "Standard Deviation")
  })
  
  output$divisional_percentage_over_under = renderValueBox({
    valueBox(value = round(percentage_over_under(),4)*100, subtitle = "Over/Under Forecasted")
  })
  
  output$terminal_graph = renderPlotly({
    df = graph_data()
    colnames(df)[2] = "Forecast"
    colnames(df)[3] = "Actual"
    g = ggplot(df, aes(Date))+geom_line(aes(y=Forecast, colour = "Forecast"))+geom_line(aes(y=Actual, colour="Actual"))
    ggplotly(g)
  })
  
  output$terminal_accuracy = renderValueBox({
    valueBox(value = round(accuracy(),4)*100, subtitle = "Accuracy")
  })
  
  output$terminal_st_dev = renderValueBox({
    valueBox(value = round(st_dev(),4)*100, subtitle = "Standard Deviation")
  })
  
  output$terminal_percentage_over_under = renderValueBox({
    valueBox(value = round(percentage_over_under(),4)*100, subtitle = "Over/Under Forecasted")
  })
  
  output$terminal_accuracy_rankings = renderDataTable({
    terminal_table_data()
  })
}

shinyApp(ui, server)
