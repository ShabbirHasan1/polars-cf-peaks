package main

import (
	"fmt"
	"gopeaks/gopeaks"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var source_df sync.RWMutex
var result_df sync.RWMutex

func main() {
	start_time := time.Now()

	var df gopeaks.Dataframe

	df.Log_File_Name = "Outbox/Gopeaks_Log_" +
		start_time.Format("060102_150405") + ".csv"

	gopeaks.Create_Log(df)

	gopeaks.View_Sample("Inbox/Master.csv")

	source_file_path := gopeaks.Get_CLI_file_path("10M-Fact.csv")

	gopeaks.View_Sample(source_file_path)
	df.Partition_Size_MB = 5
	df.Thread = 100		

	result_file_path := "Outbox/Gopeaks_Turbo_Innerjoin_Result" + filepath.Base(source_file_path)

	_, err := os.Create(result_file_path)
	if err != nil {
		fmt.Println("Fail to create file")
		return
	}

	fmt.Println()
	fmt.Println("*** Processing: gopeaks_turbo_innerjoin.exe ***")
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

	read_batch := make(map[int]int)
	write_all_batch := make(map[int]int)
	source_df_group := make(map[int]gopeaks.Dataframe)
	result_df_group := make(map[int]gopeaks.Dataframe)

	go ReadFile(ref_df, read_batch, source_df_group, source_file_path)

	for ref_df.Processed_Partition < ref_df.Partition_Count {

		for len(read_batch) <= ref_df.Streaming_Batch || len(result_df_group) > 5 {
			time.Sleep(100 * time.Millisecond)
		}

		df := source_df_group[ref_df.Streaming_Batch]
		df = *gopeaks.Join_Key_Value(df, *master_df, "Product, Style => Inner(KeyValue)")
		df = *gopeaks.Add_Column(df, "Quantity, Unit_Price => Multiply(Amount)")		

		result_df.Lock()
		result_df_group[ref_df.Streaming_Batch] = df
		result_df.Unlock()

		if ref_df.Streaming_Batch == 0 {
			go WriteFile(result_df_group, write_all_batch, total_batch, result_file_path)
		}

		source_df.Lock()
		if _, found := source_df_group[ref_df.Streaming_Batch]; found {
			delete(source_df_group, ref_df.Streaming_Batch)
		}
		source_df.Unlock()

		ref_df.Processed_Partition += df.Thread
		ref_df.Streaming_Batch += 1
	}

	for len(write_all_batch) < 1 {
		time.Sleep(100 * time.Millisecond)
	}
}

func ReadFile(ref_df gopeaks.Dataframe, read_batch map[int]int, source_df_group map[int]gopeaks.Dataframe, source_file_path string) {

	ref_df = *gopeaks.Get_CSV_Partition_Address(ref_df, source_file_path)

	ref_df.Processed_Partition = 0
	ref_df.Streaming_Batch = 0

	for ref_df.Processed_Partition < ref_df.Partition_Count {

		df := gopeaks.Read_CSV(ref_df, source_file_path)

		source_df.Lock()
		source_df_group[ref_df.Streaming_Batch] = *df
		source_df.Unlock()

		read_batch[ref_df.Streaming_Batch] = 1

		for len(source_df_group) > 5 {
			time.Sleep(100 * time.Millisecond)
		}

		ref_df.Processed_Partition += df.Thread
		ref_df.Streaming_Batch += 1
	}
}

func WriteFile(result_df_group map[int]gopeaks.Dataframe, write_all_batch map[int]int, total_batch int, result_file_path string) {

	for batch := 0; batch < total_batch; batch++ {

		if _, found := result_df_group[batch]; found {

			gopeaks.Append_CSV(result_df_group[batch], result_file_path)
			fmt.Print(batch+1, " ")

			result_df.Lock()
			delete(result_df_group, batch)
			result_df.Unlock()

		} else {
			time.Sleep(100 * time.Millisecond)
			batch--
		}
	}

	write_all_batch[0] = 1
}
