package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const logGenTimeTicker = 100

var client http.Client = http.Client{
	Timeout: 45 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		MaxConnsPerHost:     0,
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     3 * time.Second,
	},
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var lowercaseLetters = []rune("abcdefghijklmnopqrstuvwxyz")

var count = 0
var timeCounter = 1

var currentSize, fileCount, filesToWrite int = 0, 1, 0
var lenOfSequence int = 4
var filenameSequences = []string{""}
var noOfSequences = 0

var totalRollOverSize, totalLogSize float64
var filename string
var keysToFilter []string

var stopFileWriting = false

var timeline TimelineUpdate
var extendedTags []extendedTagsStruct

// Config ...
type Config struct {
	IP                 string                     `json:"ip"`
	KafkaTopics        []string                   `json:"kafka_topics"`
	AuthToken          string                     `json:"auth_token"`
	Tags               map[string]string          `json:"tags"`
	VarTags            map[string][]string        `json:"varTags"`
	MaxBulkCountFile   uint64                     `json:"max_bulk_count_file"`
	MaxBulkCountKafka  uint64                     `json:"max_bulk_count_kafka"`
	MaxBulkSize        uint64                     `json:"max_bulk_size"`
	LogsPerMinKafka    uint64                     `json:"logs_per_min_kafka"`
	LogsPerMinFile     uint64                     `json:"logs_per_min_file"`
	RollOverSize       float64                    `json:"rollOverSize"`
	FlushInterval      uint64                     `json:"flush_interval"`
	SaveLogsToFile     string                     `json:"save_logs_onto_file"`
	SendToKafka        string                     `json:"send_json_logs_to_Kafka"`
	FileName           string                     `json:"fileName"`
	ParallelGeneration string                     `json:"parallelGeneration"`
	FilterTags         []string                   `json:"filterTags"`
	TotalLogSize       float64                    `json:"totalLogSize"`
	PastData           map[string]int             `json:"pastData"`
	DaySpeed           int                        `json:"daySpeed"`
	ExtendedTags       []configExtendedTagsStruct `json:"extTags"`
}

// TimelineUpdate ...
type TimelineUpdate struct {
	logGenPassedTime int
	mux              sync.Mutex
}

type configExtendedTagsStruct struct {
	TagName    string `json:"key"`
	TagValue   string `json:"value"`
	LifePeriod string `json:"life_period"`
}

type extendedTagsStruct struct {
	tagName         string
	tagValue        string
	startTime       time.Time // start time
	lifePeriodHours float64   // period in hours after which to end
}

func getRandomLog(allLogs []string, config *Config) (map[string]interface{}, map[string]interface{}) {

	rand.Seed(time.Now().UnixNano())
	indexToUse := rand.Intn(len(allLogs))

	logWithLevel := strings.SplitN(allLogs[indexToUse], ",", 2)

	record := make(map[string]interface{})
	record["level"] = strings.ToLower(strings.TrimSpace(strings.SplitN(logWithLevel[0], "=", 2)[1]))
	record["time"] = getTime(getUpdatedDate(config), config).Unix() * 1000

	record = addVarTags(record, config)
	record = addExtTags(record, extendedTags)

	message := strings.TrimSpace(strings.SplitN(logWithLevel[1], "=", 2)[1])
	message = getRandomValue(message)

	record["message"] = message

	kafkaRecord := make(map[string]interface{})
	kafkaRecord["value"] = record

	return kafkaRecord, record
}

func addVarTags(record map[string]interface{}, config *Config) map[string]interface{} {
	for key, arr := range config.VarTags {
		arrSize := len(arr)
		randElement := getRandomValue(arr[rand.Intn(arrSize)])
		record[key] = randElement
	}
	for key, value := range config.Tags {
		if _, ok := record[key]; !ok {
			record[key] = value
		}
	}
	return record
}

func addExtTags(record map[string]interface{}, extTags []extendedTagsStruct) map[string]interface{} {
	for _, et := range extTags {
		elapsedHours := time.Since(et.startTime).Hours()
		if elapsedHours < et.lifePeriodHours {
			record[et.tagName] = et.tagValue
		}
	}
	return record
}

func getRandomValue(val string) string {
	message := strings.ReplaceAll(val, "$IP", fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256)))
	message = strings.ReplaceAll(message, "$INT", fmt.Sprintf("%d", rand.Intn(65535)+1))
	message = strings.ReplaceAll(message, "$STRING", func(n int) string {
		b := make([]rune, n)
		for i := range b {
			b[i] = letters[rand.Intn(len(letters))]
		}
		return string(b)
	}(10))
	return message
}

func sendToFile(logsToSave []map[string]interface{}, config *Config, targetFile string) {
	tempsize, err := findCurrentSize(logsToSave)
	if err != nil {
		log.Fatal(err)
		return
	}
	convertedTempsizeinGB := float64(tempsize) / 1073741824
	if config.RollOverSize >= convertedTempsizeinGB {
		if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
			fmt.Printf("Started writing logs to file %s\n", filename)
		}
	} else {
		fmt.Printf("the rollover size %f GB is lesser than the current log batch size %f GB\n", config.RollOverSize, convertedTempsizeinGB)
		fmt.Printf("the file %s was not created ,increase the rollover size or decrease the max_bulk_count,and start again.\n", filename)
		os.Exit(0)
	}
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
		return
	}
	// fmt.Printf("writing batch of %d logs to file %s\n", len(logsToSave), filename)

	if !stopFileWriting {
		checkLogSize(tempsize, file, targetFile, config)

		for _, v := range logsToSave {
			fileData, err := json.Marshal(v)
			if err != nil {
				log.Print(err)
				return
			}
			_, err = fmt.Fprintf(file, "%s\n", fileData)
			if err != nil {
				fmt.Println(err)
				file.Close()
				return
			}
		}
	}

	if err := file.Close(); err != nil {
		log.Fatal(err)
		return
	}
}

func sendToKafka(logsToSend []map[string]interface{}, config *Config, topicName string) {

	kafkaURL := fmt.Sprintf("%s/%s", strings.TrimSuffix(config.IP, "/"), topicName)
	kafkaRecords := make(map[string]interface{})
	kafkaRecords["records"] = logsToSend

	kafkaData, err := json.Marshal(kafkaRecords)
	if err != nil {
		log.Print(err)
		return
	}

	fmt.Printf("Sending %d logs / %d bytes to Kafka\n", len(logsToSend), len(kafkaData))

	req, err := http.NewRequest("POST", kafkaURL, bytes.NewBuffer(kafkaData))
	if err != nil {
		fmt.Println(err)
		return
	}

	req.Header.Set("Content-Type", "application/vnd.kafka.json.v2+json")
	req.Header.Set("Content-Type", "application/vnd.kafka.json.v2+json")
	req.Header.Set("Accept", "application/vnd.kafka.v2+json,application/json")
	req.Header.Set("Authorization", config.AuthToken)

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		fmt.Printf("Failed to send Kafka records due to code:%s,response:%v\n", res.Status, res.Body)
	}
}

func generateLogsForOneMinute(startTime time.Time, config *Config, allLogs []string, target string) {

	totalLogsToSend := int(config.LogsPerMinKafka)
	sizeCheckCounter := 0
	for range time.Tick(time.Duration(config.FlushInterval) * time.Second) {

		logsToSendInThisFlush := int(math.Ceil((float64(config.LogsPerMinKafka) / float64(60/config.FlushInterval))))
		logsToSend := make([]map[string]interface{}, 0)

		for {
			kafkaRecord, _ := getRandomLog(allLogs, config)
			logsToSend = append(logsToSend, kafkaRecord)
			logsToSendInThisFlush--
			totalLogsToSend--
			sizeCheckCounter++

			if func() bool {

				if logsToSendInThisFlush <= 0 {
					return true
				}

				if len(logsToSend) >= int(config.MaxBulkCountKafka) {
					return true
				}

				kafkaRecords := make(map[string]interface{})
				kafkaRecords["records"] = logsToSend

				if math.Mod(float64(sizeCheckCounter), 10.0) == 0 {
					kafkaData, err := json.Marshal(kafkaRecords)
					if err != nil {
						log.Print(err)
						os.Exit(1)
					}

					size := len(kafkaData)

					if size >= int(config.MaxBulkSize) {
						return true
					}
				}

				return false
			}() {
				copyOfLogsToSend := make([]map[string]interface{}, len(logsToSend))
				copy(copyOfLogsToSend, logsToSend)
				logsToSend = make([]map[string]interface{}, 0)

				go sendToKafka(copyOfLogsToSend, config, target)

				if totalLogsToSend <= 0 {
					fmt.Printf("Completed sending %d logs to kafka in %f minutes\n", int(config.LogsPerMinKafka)-totalLogsToSend, time.Since(startTime).Minutes())
					return
				} else if logsToSendInThisFlush <= 0 {
					break
				}
			}

		}
	}
}
func generateFileLogsForOneMinute(startTime time.Time, config *Config, allLogs []string, target string) {
	totalFileLogsToSend := int(config.LogsPerMinFile)
	filelogsToSend := make([]map[string]interface{}, 0)
	remainingfilelogs := int(config.LogsPerMinFile) % int(config.MaxBulkCountFile)
	for range time.Tick(time.Duration(config.FlushInterval) * time.Second) {

		for {
			_, fileRecord := getRandomLog(allLogs, config)
			filteredFileRecord := filterData(fileRecord)
			filelogsToSend = append(filelogsToSend, filteredFileRecord)
			totalFileLogsToSend--

			if func() bool {
				if len(filelogsToSend) >= int(config.MaxBulkCountFile) {
					return true
				}
				if totalFileLogsToSend <= 0 {
					if len(filelogsToSend) >= remainingfilelogs {
						return true
					}
				}

				return false
			}() {

				copyOfFilelogsToSend := make([]map[string]interface{}, len(filelogsToSend))
				copy(copyOfFilelogsToSend, filelogsToSend)
				filelogsToSend = make([]map[string]interface{}, 0)
				if !stopFileWriting {
					if config.ParallelGeneration == "true" {
						go sendToFile(copyOfFilelogsToSend, config, target)
					} else {
						sendToFile(copyOfFilelogsToSend, config, target)
					}
				}
				if totalFileLogsToSend <= 0 {
					fmt.Printf("Sent %d logs to file in %f minutes\n", int(config.LogsPerMinFile)-totalFileLogsToSend, time.Since(startTime).Minutes())
					return
				}

			}
		}
	}
	filelogsToSend = make([]map[string]interface{}, 0)
}

func processLogTemlates(args []string) []string {

	var allLogs []string

	noOfLogTemplates := len(args) - 2

	for i := 0; i < noOfLogTemplates; i++ {

		file, err := os.Open(args[i+2])

		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		defer file.Close()

		reader := bufio.NewReader(file)

		for {

			line, _, err := reader.ReadLine()
			if len(line) == 0 {
				break
			}
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}

			allLogs = append(allLogs, strings.TrimSpace(string(line)))
		}
	}
	return allLogs
}

func loadConfig(path string) (*Config, error) {

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config

	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

func findCurrentSize(logsToSave []map[string]interface{}) (int, error) {
	var totalSize int
	for _, logVal := range logsToSave {
		logData, err := json.Marshal(logVal)
		if err != nil {
			log.Print(err)
			return 0, err
		}
		logSize := len([]byte(logData))
		totalSize = totalSize + logSize
	}
	return totalSize, nil
}

func filterData(vals map[string]interface{}) map[string]interface{} {
	for _, key := range keysToFilter {
		delete(vals, key)
	}
	return vals
}

func checkLogSize(tempsize int, file *os.File, targetFile string, config *Config) {
	currentSize = currentSize + tempsize
	if currentSize > int(totalRollOverSize) {
		if fileCount < filesToWrite || config.TotalLogSize == -1 {
			fileCount = fileCount + 1
			// currentSize = tempsize
			currentSize = 0
			filename = targetFile + "." + filenameSequences[fileCount] + "." + strconv.Itoa(fileCount)
		} else {
			fileStat, err := file.Stat()
			if err != nil {
				log.Fatal(err)
			}
			lastFileSize := fileStat.Size()
			if lastFileSize >= int64(totalRollOverSize) {
				fmt.Println("Generated logs of total size of", config.TotalLogSize, "GB. So stopping the process of file writing.")
				if config.SendToKafka == "false" {
					os.Exit(0)
				} else {
					stopFileWriting = true
					return
				}
			}
		}
	}
}

func getUpdatedDate(config *Config) time.Time {
	oldDate := time.Now().AddDate(-config.PastData["years"], -config.PastData["months"], -config.PastData["days"])
	return oldDate
}

func getTime(t time.Time, config *Config) time.Time {
	timeline.mux.Lock()
	defer timeline.mux.Unlock()
	logGenTime := t.Add(time.Duration(timeline.logGenPassedTime) * time.Millisecond)
	return logGenTime
}

func trackTime(config *Config) {
	for range time.Tick(logGenTimeTicker * time.Millisecond) {
		timePassed := logGenTimeTicker * timeCounter
		timeline.mux.Lock()
		timeline.logGenPassedTime = timePassed * config.DaySpeed
		timeline.mux.Unlock()
		timeCounter++
	}
}

func getLifePeriodInHours(lifePeriod string) float64 {
	var lpHours float64
	timeUnit := lifePeriod[len(lifePeriod)-1:]
	timeDuration, err := strconv.ParseFloat(lifePeriod[:len(lifePeriod)-1], 64)
	if err != nil {
		fmt.Println("Error in parsing life_period value.")
		log.Fatal(err)
	}
	if timeUnit == "h" {
		lpHours = timeDuration
	} else if timeUnit == "d" {
		lpHours = 24 * timeDuration
	} else {
		log.Fatal("Invalid time unit given for life period of the extended tags. Use either h(hours) or d(days).")
	}
	return lpHours
}

func initializeFileName(lenOfSequence int, noOfSequences int) {
	alphabets := 26
	getFilenamesInSequence("", alphabets, lenOfSequence)
}

func getFilenamesInSequence(prefix string, alphabets int, lenOfSequence int) {
	if lenOfSequence == 0 {
		noOfSequences = noOfSequences - 1
		filenameSequences = append(filenameSequences, prefix)
		return
	}

	for i := 0; i < alphabets; i++ {
		if noOfSequences == 0 {
			break
		}
		newPrefix := prefix + string(lowercaseLetters[i])
		getFilenamesInSequence(newPrefix, alphabets, lenOfSequence-1)
	}
}

func initExtendedTags(config *Config) {
	for _, t := range config.ExtendedTags {
		var extTags extendedTagsStruct
		extTags.tagName = t.TagName
		extTags.tagValue = t.TagValue
		extTags.lifePeriodHours = getLifePeriodInHours(t.LifePeriod)
		extTags.startTime = time.Now().Add(1 * time.Minute) // because log generation starts after 1 minute
		extendedTags = append(extendedTags, extTags)
	}
}

func initValues(config *Config) {
	//totalRollOverSize := config.RollOverSize * 1000000000
	totalRollOverSize = config.RollOverSize * 1073741824
	if config.TotalLogSize > 0 {
		totalLogSize = config.TotalLogSize * 1073741824
		filesToWrite = int(config.TotalLogSize / config.RollOverSize)
		noOfSequences = filesToWrite
		initializeFileName(lenOfSequence, noOfSequences)
	}
	filename = config.FileName + "." + filenameSequences[fileCount] + "." + strconv.Itoa(fileCount)
	keysToFilter = config.FilterTags
}

func main() {

	args := os.Args
	if len(args) < 3 {
		fmt.Println("Insufficient arguments")
		return
	}

	config, err := loadConfig(args[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	initValues(config)

	if len(config.KafkaTopics) <= 0 || config.FlushInterval > 60 || config.MaxBulkCountKafka > config.LogsPerMinKafka || config.MaxBulkCountFile > config.LogsPerMinFile {
		fmt.Println("Either 'kafka_topics is empty' or 'flush_interval is greater than 60' or 'max_bulk_count is greater than logs_per_min'")
		return
	}
	if config.TotalLogSize == 0 {
		fmt.Println("the totalLogSize is 0 set -1 for continuous log files generation or set a proper value")
		return
	}

	allLogs := processLogTemlates(args)

	if config.DaySpeed > 1 {
		go trackTime(config)
	}

	initExtendedTags(config)

	for range time.Tick(time.Minute) {
		if config.SendToKafka == "true" && config.SaveLogsToFile == "true" {
			if !stopFileWriting {
				go generateFileLogsForOneMinute(time.Now(), config, allLogs, config.FileName)
			}
			for _, topicName := range config.KafkaTopics {
				fmt.Printf("Starting to send 1 minute logs to topic %s\n", topicName)
				go generateLogsForOneMinute(time.Now(), config, allLogs, topicName)
			}
		} else if config.SendToKafka == "true" {
			for _, topicName := range config.KafkaTopics {
				fmt.Printf("Starting to send 1 minute logs to topic %s\n", topicName)
				go generateLogsForOneMinute(time.Now(), config, allLogs, topicName)
			}
		} else if config.SaveLogsToFile == "true" && !stopFileWriting {
			go generateFileLogsForOneMinute(time.Now(), config, allLogs, config.FileName)
		}
	}
}
