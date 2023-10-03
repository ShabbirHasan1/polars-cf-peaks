package main

import (
	"fmt"		
	"path/filepath"
	"peakgo/peakgo"	
	"time"
)

func main() {	

	start_time := time.Now()

	var df peakgo.Dataframe
	df.Log_File_Name = "Outbox/Log-" + start_time.Format("060102-150405") + ".csv"
	df.Partition_Size_MB = 5
	df.Thread = 100

	peakgo.Create_Log(df)	

	query1 := peakgo.Query(
		"filter", "Style(=F)",
		"build_key_value", "Product, Style => Table(key_value)")

	master_df := peakgo.Run_Batch(df, "Inbox/Master.csv", query1)

	query2 := peakgo.Query(		
		"filter", "Shop(S90..S99)",
		"join_key_value", "Product, Style => Inner(key_value)",				
		"add_column", "Quantity, Unit_Price => Multiply(Amount)",
		"filter", "Amount:Float(>100000)",
        "group_by", "Shop, Product => Count() Sum(Quantity) Sum(Amount)")
	
	source_file := peakgo.Get_CLI_file_path("10-MillionRows.csv")
	result_file := []string{"Outbox/PeakGo-Detail-Result-" + filepath.Base(source_file), 
	                        "Outbox/Peakgo-Summary-Result-" + filepath.Base(source_file)}
							
	peakgo.Run_Stream(df, &master_df, source_file, query2, result_file)	

	peakgo.View_Sample(result_file[0])
	peakgo.View_Sample(result_file[1])

	end_time := time.Now()
	duration := end_time.Sub(start_time)	
	fmt.Printf("Peakgo Duration (In Second): %.3f \n", duration.Seconds())
}