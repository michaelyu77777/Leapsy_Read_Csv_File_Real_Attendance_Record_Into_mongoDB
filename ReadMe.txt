程式說明:匯入今日打卡紀錄(會先刪除掉舊的)(FOR排程)

1.我們使用絕對路徑放置config.json。(D:\10_read_daily_clock_in_record_into_mongodb\config.json)

2.原因:Win10的工作排程器自動執行時，預設路徑會在 C:\Windows\System32\ (並非跟執行檔exe相同的資料夾下)，因此我們不使用相對路徑。

3.排程所產生的log_err, log_info會在 C:\Windows\System32\10_read_daily_clock_in_record_into_mongodb\