package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"time"

	//"labix.org/v2/mgo"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs" //Log寫入設定
	"github.com/rifflock/lfshook"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"                    //寫log檔
	"golang.org/x/text/encoding/traditionalchinese" // 繁體中文編碼
	"golang.org/x/text/transform"
)

//設定檔
var config Config = Config{}
var worker = runtime.NumCPU()

// 指定編碼:將繁體Big5轉成UTF-8才會正確
var big5ToUTF8Decoder = traditionalchinese.Big5.NewDecoder()

// 設定檔
type Config struct {
	MongodbServer             string   // DB IP:port
	DBName                    string   // 資料庫名
	CollectionName            string   // 資料表名
	DailyRecordFileFolderPath string   // CSV目錄資料夾路徑
	ScheduleTime              []string // 排程時間
	// ScheduleTime1             string   // 排程一時間
	// ScheduleTime2             string   // 排程二時間
	// ScheduleTime3             string   // 排程三時間
}

// 日打卡紀錄檔
type DailyRecord struct {
	Date       string    `json:"date"`
	Name       string    `json:"name"`
	CardID     string    `json:"cardID"`
	Time       string    `json:"time"`
	Message    string    `json:"msg"`
	EmployeeID string    `json:"employeeID"`
	DateTime   time.Time `json:"dateTime"`
}

func main() {

	runtime.GOMAXPROCS(worker)
	log_info.Info("Logic CPU 數量:", worker)
	// runtime.GOMAXPROCS(runtime.NumCPU())

	// 每天定時讀取寫入資料(幾時幾分幾秒)
	// parser := cron.NewParser(
	// 	cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	// )

	// c := cron.New(cron.WithParser(parser))
	c := cron.New()

	// 取得排程
	ScheduleTime := config.ScheduleTime

	// 看有幾個排程就定幾個排程
	for i, timeString := range ScheduleTime {

		log_info.Infof("新增排程%d:時間為<%s>", i+1, timeString)

		c.AddFunc(timeString, func() {
			log_info.Infof("執行排程%d:時間為<%s>", i+1, timeString)
			ImportDailyRecord()
		})
	}

	// 定時字串: 秒 分 時 日 月 年
	// timing := []string{config.ScheduleTime1,
	// 	config.ScheduleTime2,
	// 	config.ScheduleTime3}

	// c.AddFunc(timing[0], func() {
	// 	log_info.Infof("排程一<%s>時間到", timing[0])
	// 	ImportDailyRecord()
	// })

	// c.AddFunc(timing[1], func() {
	// 	log_info.Infof("排程二<%s>時間到:", timing[1])
	// 	ImportDailyRecord()
	// })

	// c.AddFunc(timing[2], func() {
	// 	log_info.Infof("排程三<%s>時間到:", timing[2])
	// 	ImportDailyRecord()
	// })

	c.Start()

	for {
		//select {}
		time.Sleep(time.Second)
	}
}

// init 制定LOG層級(自動呼叫?)
// func init() {
// 	//log輸出為json格式
// 	logrus.SetFormatter(&logrus.JSONFormatter{})
// 	//輸出設定為標準輸出(預設為stderr)
// 	logrus.SetOutput(os.Stdout)
// 	//設定要輸出的log等級
// 	logrus.SetLevel(logrus.DebugLevel)
// }

//Log檔
var log_info *logrus.Logger
var log_err *logrus.Logger

//var writer *rotatelogs.RotateLogs

/*
 * 初始化：執行main之前就會執行
 */
func init() {

	fmt.Println("執行init()初始化")

	// 抓設定檔參數

	/**設定LOG檔層級與輸出格式*/
	// logPath := "/log"  // 單斜線表示 main所在硬碟槽之根目錄 例如D：目錄下
	logPath := "./log" // 點斜線表示 main所在資料夾目錄
	pathInfo := logPath + "/info/info"
	pathErr := logPath + "/err/err"

	writer, _ := rotatelogs.New(
		pathInfo+".%Y%m%d%H",                        // 檔名格式
		rotatelogs.WithLinkName(pathInfo),           // 生成软链，指向最新日志文件
		rotatelogs.WithMaxAge(10080*time.Minute),    // 文件最大保存時間(保留七天)
		rotatelogs.WithRotationTime(60*time.Minute), // 日誌切割時間間隔(一小時存一個檔案)
	)

	// 設定LOG等級
	pathMap := lfshook.WriterMap{
		logrus.InfoLevel: writer,
		//logrus.PanicLevel: writer, //若執行發生錯誤則會停止不進行下去
	}

	// 加入到log_info
	log_info = logrus.New()
	log_info.Hooks.Add(lfshook.NewHook(pathMap, &logrus.JSONFormatter{})) //Log檔綁訂相關設定

	// Error層級
	writer, _ = rotatelogs.New(
		pathErr+".%Y%m%d%H",                         // 檔名格式
		rotatelogs.WithLinkName(pathErr),            // 生成软链，指向最新日志文件
		rotatelogs.WithMaxAge(10080*time.Minute),    // 文件最大保存時間(保留七天)
		rotatelogs.WithRotationTime(60*time.Minute), // 日誌切割時間間隔(一小時存一個檔案)
	)

	// 設定LOG等級
	pathMap = lfshook.WriterMap{
		logrus.ErrorLevel: writer,
		//logrus.PanicLevel: writer, //若執行發生錯誤則會停止不進行下去
	}

	// 加入到log_err
	log_err = logrus.New()
	log_err.Hooks.Add(lfshook.NewHook(pathMap, &logrus.JSONFormatter{})) //Log檔綁訂相關設定

	/**讀設定檔(config.json)*/
	log_info.Info("讀取設定檔Config")
	file, err := os.Open("config.json") //取相對路徑
	//file, _ := os.Open("config.json")
	//file, err := os.Open("D:\\10_read_daily_clock_in_record_into_mongodb_config\\config.json") //取相對路徑
	//file, err := os.Open("D:\\workspace-GO\\Leapsy_Env\\10_OK_讀取日打卡紀錄+寫入mongoDB(當日檔案)\\config.json")

	buf := make([]byte, 2048)
	if err != nil {
		log_err.WithFields(logrus.Fields{
			"trace": "trace-0001",
			"err":   err,
		}).Error("-打開設定檔config錯誤")
	}

	n, err := file.Read(buf)
	fmt.Println(string(buf))
	if err != nil {
		log_err.WithFields(logrus.Fields{
			"trace": "trace-0002",
			"err":   err,
		}).Error("-讀取設定檔config錯誤")
		panic(err)
	}

	log_info.Info("轉換設定檔config轉成json")
	err = json.Unmarshal(buf[:n], &config)
	if err != nil {
		log_err.WithFields(logrus.Fields{
			"trace": "trace-0003",
			"err":   err,
		}).Error("轉換config成json發生錯誤")
		panic(err)
	}
}

// ImportDailyRecord :主程式-每日打卡資料
func ImportDailyRecord() {

	//先算出要抓今日或昨日:年月日時
	currentTime := time.Now()

	//指定年月日
	date := ""

	//若現在是九點前:取昨日
	if currentTime.Hour() < 9 {
		log_info.Info("九點前:取昨日(hour=", currentTime.Hour())

		yesterday := currentTime.AddDate(0, 0, -1)
		date = yesterday.Format("20060102") //取年月日
	} else {
		//取今日
		log_info.Info("九點後:取今日(hour=", currentTime.Hour())

		date = currentTime.Format("20060102") //取年月日
	}

	//檔案名稱
	//fileName := "Rec" + year + month + day + ".csv"
	log_info.Info("取年月日:", date)

	// 移除當日所有舊紀錄
	deleteDailyRecordToday(date)

	// 建立 channel 存放 DailyRecord型態資料
	chanDailyRecord := make(chan DailyRecord)

	fmt.Println("測試chanDailyRecord size=", len(chanDailyRecord))

	// 標記完成
	dones := make(chan struct{}, worker)

	// 同時 chanDailyRecord <- 「將日打卡紀錄csv檔案內容讀出」放入channel
	go addDailyRecordToChannel(chanDailyRecord, date)

	// 同時 mongodb資料庫 <- chanDailyRecord 讀出資料，寫入DB
	for i := 0; i < worker; i++ {
		go insertDailyRecord(chanDailyRecord, dones)
	}
	//等待完成
	awaitForCloseResult(dones)
	log_info.Info("日打卡紀錄插入完畢")
}

/**
 * 刪除當日所有舊紀錄
 */

func deleteDailyRecordToday(date string) {

	log_info.Info("連接MongoDB")
	session, err := mgo.Dial(config.MongodbServer)
	defer session.Close()
	if err != nil {
		log_err.WithFields(logrus.Fields{
			"trace": "trace-0004",
			"err":   err,
		}).Error("連接MongoDB發生錯誤(要刪除日打卡記錄時)")

		panic(err)
	}

	c := session.DB(config.DBName).C(config.CollectionName)
	//c := session.DB("leapsy_env").C("dailyRecord_real")

	log_info.WithFields(logrus.Fields{
		"MongodbServer":             config.MongodbServer,
		"DBName":                    config.DBName,
		"CollectionName":            config.CollectionName,            //date
		"DailyRecordFileFolderPath": config.DailyRecordFileFolderPath, //name
	}).Info("設定檔:")

	log_info.Info("移除當日所有舊紀錄,日期為 date: ", date)
	info, err := c.RemoveAll(bson.M{"date": date}) //移除今天所有舊的紀錄(格式年月日)
	if err != nil {
		log_err.WithFields(logrus.Fields{
			"trace": "trace-0005",
			"err":   err,
			"date":  date,
		}).Error("移除當日所有舊紀錄失敗")

		os.Exit(1)
	}

	log_info.Info("發生改變的info: ", info)

}

/*
 * 讀取今日打卡資料 加入到channel中
 * 讀取的檔案().csv 或 .txt檔案)，編碼要為UTF-8，繁體中文才能正確被讀取
 */
func addDailyRecordToChannel(chanDailyRecord chan<- DailyRecord, date string) {

	//指定要抓的csv檔名
	fileName := "Rec" + date + ".csv"

	log_info.Info("打開CSV文件", fileName)

	// 打開每日打卡紀錄檔案(windows上面登入過目的資料夾，才能運行)
	// file, err := os.Open("Z:\\" + fileName)
	// file, err := os.Open("\\\\leapsy-nas3\\CheckInRecord\\" + fileName)
	file, err := os.Open(config.DailyRecordFileFolderPath + fileName)
	// 最後回收資源
	defer file.Close()

	if err != nil {
		log_err.WithFields(logrus.Fields{
			"trace":    "trace-0006",
			"err":      err,
			"date":     date,
			"fileName": fileName,
		}).Error("打開.csv文件失敗")

		return
	}

	log_info.Info("讀取.csv文件")

	// 讀檔
	reader := csv.NewReader(file)

	//行號
	counter := 0

	// 一行一行讀進來
	for {

		line, err := reader.Read()
		counter++

		// 若讀完了
		if err == io.EOF {

			//close(chanDailyRecord)
			log_info.Info("csv文件讀取完成")
			break

		} else if err != nil {

			//close(chanDailyRecord)
			fmt.Println("關閉channel")

			log_err.WithFields(logrus.Fields{
				"trace":    "trace-0007",
				"err":      err,
				"date":     date,
				"fileName": fileName,
			}).Error("讀取csv文件失敗")
			fmt.Println("Error:", err)

			break
		}

		// 處理Name編碼問題: 將繁體(Big5)轉成 UTF-8，儲存進去才正常
		big5Name := line[1]                                             // Name(Big5)
		utf8Name, _, _ := transform.String(big5ToUTF8Decoder, big5Name) // 轉成 UTF-8
		//fmt.Println(utf8Name) // 顯示"名字"

		date := line[0]
		name := utf8Name
		cardID := line[2]
		time := line[3]
		msg := line[4]
		employeeID := line[5]
		dateTime := getDateTime(date, time)

		// 建立每筆DailyRecord物件
		dailyrecord := DailyRecord{
			date,
			name,
			cardID,
			time,
			msg,
			employeeID,
			dateTime} // 建立每筆DailyRecord物件

		log_info.WithFields(logrus.Fields{
			"檔名":         fileName,
			"行號":         counter,
			"date":       date,       //date
			"name":       name,       //name
			"cardID":     cardID,     //cardID
			"time":       time,       //time
			"msg":        msg,        //msg
			"employeeID": employeeID, //employeeID
			"dateTime":   dateTime,
		}).Info("讀入第<" + strconv.Itoa(counter) + ">行紀錄:")

		chanDailyRecord <- dailyrecord // 存到channel裡面
	}
}

/*
 * 將所有日打卡紀錄，全部插入到 mongodb
 */
func insertDailyRecord(chanDailyRecord <-chan DailyRecord, dones chan<- struct{}) {
	//开启loop个协程

	log_info.Info("連接MongoDB")

	session, err := mgo.Dial(config.MongodbServer)
	defer session.Close()

	//error
	if err != nil {
		log_err.WithFields(logrus.Fields{
			"trace": "trace-0008",
			"err":   err,
		}).Error("連接MongoDB")

		panic(err)
	}

	// 設定資料庫名,資料表名
	c := session.DB(config.DBName).C(config.CollectionName)

	log_info.WithFields(logrus.Fields{
		"MongodbServer":             config.MongodbServer,             //IP:port
		"DBName":                    config.DBName,                    //資料庫
		"CollectionName":            config.CollectionName,            //資料表
		"DailyRecordFileFolderPath": config.DailyRecordFileFolderPath, //CSV檔案路徑
	}).Info("設定檔:")

	for dailyrecord := range chanDailyRecord {
		c.Insert(&dailyrecord)
		log_info.Info("插入一筆資料到DB：", dailyrecord)
	}

	dones <- struct{}{}
}

/** 組合年月+時間 */
func getDateTime(myDate string, myTime string) time.Time {

	// fmt.Println("myDate=", myDate)
	// fmt.Println("myTime=", myTime)

	//ex:20201104
	year, err := strconv.Atoi(myDate[0:4])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 year=%d", year)
		log_err.WithFields(logrus.Fields{
			"err":  err,
			"year": year,
		}).Error("字串轉換數字錯誤")
	}
	// fmt.Println("year=", year)

	month, err := strconv.Atoi(myDate[4:6])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 month=%d", month)
		log_err.WithFields(logrus.Fields{
			"err":   err,
			"month": month,
		}).Error("字串轉換數字錯誤")
	}
	// fmt.Println("month=", month)

	day, err := strconv.Atoi(myDate[6:8])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 day=%d", day)
		log_err.WithFields(logrus.Fields{
			"err": err,
			"day": day,
		}).Error("字串轉換數字錯誤")
	}
	// fmt.Println("day=", day)

	//ex:1418
	hr, err := strconv.Atoi(myTime[0:2])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 hr=%d", hr)
		log_err.WithFields(logrus.Fields{
			"err": err,
			"hr":  hr,
		}).Error("字串轉換數字錯誤")
	}
	// fmt.Println("hr=", hr)

	min, err := strconv.Atoi(myTime[2:4])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 min=%d", min)
		log_err.WithFields(logrus.Fields{
			"err": err,
			"min": min,
		}).Error("字串轉換數字錯誤")
	}
	// fmt.Println("min=", min)

	//sec, err := strconv.Atoi(myTime[6:8])
	// if nil != err {
	// 	fmt.Printf("字串轉換數字錯誤 sec=", sec)
	// }

	sec := 0

	// msec, err := strconv.Atoi("0")
	// if nil != err {
	// 	fmt.Printf("字串轉換數字錯誤 msec=", msec)
	// }

	msec := 0

	fmt.Println("year=", year, ",month=", month, ",day=", day, ",hr=", hr, ",sec=", sec, ",msec=", msec)

	t := time.Date(year, time.Month(month), day, hr, min, sec, msec, time.Local)
	fmt.Printf("組成時間:%+v\n", t)
	return t

}

// 等待結束
func awaitForCloseResult(dones <-chan struct{}) {
	for {
		<-dones
		worker--
		if worker <= 0 {
			return
		}
	}
}
