package main

import (
	"fmt"
	"gopeaks/gopeaks"
	"math"
	"os"
	"path/filepath"
	"time"
)

func main() {
	start_time := time.Now()

	var df gopeaks.Dataframe

	df.Log_File_Name = "Outbox/Gopeaks_Turbo_Log_" +
		start_time.Format("060102_150405") + ".csv"

	gopeaks.Create_Log(df)

	gopeaks.View_Sample("Inbox/Master.csv")

	source_file_path := gopeaks.Get_CLI_file_path("10M-Fact.csv")

	gopeaks.View_Sample(source_file_path)
	df.Partition_Size_MB = 5
	df.Thread = 100

	result_file_path := "Outbox/Gopeaks_Innerjoin_Result_" + filepath.Base(source_file_path)

	_, err := os.Create(result_file_path)
	if err != nil {
		fmt.Println("Fail to create file")
		return
	}

	fmt.Println()
	fmt.Println("*** Processing: gopeaks_innerjoin.exe ***")
	fmt.Println()

	InnerJoin(df, source_file_path, result_file_path)

	fmt.Println()
	gopeaks.View_Sample(result_file_path)
}

func InnerJoin(ref_df gopeaks.Dataframe, source_file_path string, result_file_path string) {

	ref_df = *gopeaks.Get_CSV_Partition_Address(ref_df, "Inbox/Master.csv")
	master_df := gopeaks.Read_CSV(ref_df, "Inbox/Master.csv")
	master_df = gopeaks.Build_Key_Value(*master_df, "Product, Style => Table(KeyValue)")

	ref_df = *gopeaks.Get_CSV_Partition_Address(ref_df, source_file_path)

	total_batch_float := math.Ceil(float64(ref_df.Partition_Count) / float64(ref_df.Thread))
	total_batch := int(total_batch_float)
	fmt.Println("Partition Count: ", ref_df.Partition_Count)
	fmt.Println("Streaming Batch Count: ", total_batch)

	ref_df.Processed_Partition = 0
	ref_df.Streaming_Batch = 0

	for ref_df.Processed_Partition < ref_df.Partition_Count {

		df := gopeaks.Read_CSV(ref_df, source_file_path)		
		df = gopeaks.Join_Key_Value(*df, *master_df, "Product, Style => Inner(KeyValue)")
		df = gopeaks.Add_Column(*df, "Quantity, Unit_Price => Multiply(Amount)")

		gopeaks.Append_CSV(*df, result_file_path)

		ref_df.Processed_Partition += df.Thread
		ref_df.Streaming_Batch += 1
		fmt.Print(ref_df.Streaming_Batch, " ")
	}
}
