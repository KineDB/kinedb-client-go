package utils

import (
	"fmt"
	"strings"
	"time"

	"github.com/jinzhu/now"
)

type TimeType string

const (
	YEAR   TimeType = "YEAR"
	MONTH  TimeType = "MONTH"
	DAY    TimeType = "DAY"
	HOUR   TimeType = "HOUR"
	MINUTE TimeType = "MINUTE"
)

const GOLANG_BORN_TIME_WITH_MILLISECONDS = "2006-01-02 15:04:05.999"
const GOLANG_BORN_TIME = "2006-01-02 15:04:05"
const GOLANG_BORN_DATE = "2006-01-02"
const SYSTEM_START_TIME = "1970-01-01 00:00:00.000"

var systemStartTime = initSystemStartTime()

func initSystemStartTime() time.Time {
	systemStartTime, _ := time.Parse(GOLANG_BORN_TIME, SYSTEM_START_TIME)
	return systemStartTime
}

func SysTime() int64 {
	return time.Now().Unix()
}

func SysMilliTime() int64 {
	return time.Now().UnixMilli()
}

func SysMicroTime() int64 {
	return time.Now().UnixMicro()
}

func SysNanoTime() int64 {
	return time.Now().UnixNano()
}

func SleepSeconds(n int) {
	time.Sleep(time.Duration(n) * time.Second)
}

func PreviousTimestamp(t TimeType, n int) int64 {
	switch t {
	case YEAR:
		return time.Now().AddDate(-n, 0, 0).UnixMilli()
	case MONTH:
		return time.Now().AddDate(0, -n, 0).UnixMilli()
	case DAY:
		return time.Now().AddDate(0, 0, -n).UnixMilli()
	case HOUR:
		return time.Now().Add(time.Duration(-n) * time.Hour).UnixMilli()
	case MINUTE:
		return time.Now().Add(time.Duration(-n) * time.Minute).UnixMilli()
	default:
		return 0
	}
}

// str is "2022-09-30 10:55:59.999" or "23:59:59.999"
func ParseStrToTime(timeStr string) (time.Time, error) {
	perseTime, err := now.Parse(timeStr)
	return perseTime, err
}

// str is "2022-09-30"
func ParseStrToDate(dateStr string) (time.Time, error) {
	perseDate, err := time.Parse(GOLANG_BORN_DATE, dateStr)
	return perseDate, err
}

// "2022-09-30 10:55:59.999" to 1664506559999
func ParseTimeStrToMilliSeconds(timeStr string) (int64, error) {
	perseTime, err := now.Parse(timeStr)
	if err != nil {
		return 0, err
	}
	return perseTime.UnixMilli(), nil
}

// "2022-09-30" to 19265
func ParseDateStrToDays(dateStr string) (int32, error) {
	// only use the first 10 chars
	if len(dateStr) > 10 {
		dateStr = dateStr[:10]
	}
	psersedDate, err := time.Parse(GOLANG_BORN_DATE, dateStr)
	if err != nil {
		return 0, err
	}
	return int32(psersedDate.Sub(systemStartTime).Hours() / 24), nil
}

// 1664506559999 to "2022-09-30 10:55:59.999"
func ConvertTimestampToSimpleStr(milliseconds int64) string {
	times := time.UnixMilli(milliseconds)
	return times.Format(GOLANG_BORN_TIME)
}

// 1664506559999 to " 10:55:59.999"
func ConvertTimestampToDateStr(milliseconds int64) string {
	times := time.UnixMilli(milliseconds)
	timeStr := times.Format(GOLANG_BORN_TIME)
	return timeStr[:10]
}

// 1664506559999 to " 10:55:59.999"
func ConvertTimestampToClockStr(milliseconds int64) string {
	times := time.UnixMilli(milliseconds)
	timeStr := times.Format(GOLANG_BORN_TIME)
	return timeStr[11:]
}

// 19265 to "2022-09-30"
func ConvertDateDaysToSimpleStr(days int32) string {
	date := time.Unix(int64(days*24*60*60), 0)
	return date.Format(GOLANG_BORN_DATE)
}

// 1664506559999 to "2022-09-30 10:55:59.999"
func ConvertTimestampToSimpleStrWithMiliiseconds(milliseconds int64) string {
	times := time.UnixMilli(milliseconds)
	formattedTime := times.Format(GOLANG_BORN_TIME_WITH_MILLISECONDS)
	if !strings.Contains(formattedTime, ".") {
		formattedTime = formattedTime + ".000"
	}
	return formattedTime
}

func ConvertTimestampToMonthLastDaySimpleStr(milliseconds int64) string {
	times := time.UnixMilli(milliseconds)
	currentYear, currentMonth, _ := times.Date()
	currentLocation := times.Location()
	firstOfMonth := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, currentLocation)
	lastOfMonth := firstOfMonth.AddDate(0, 1, -1)
	return lastOfMonth.Format(GOLANG_BORN_TIME)
}

func ConvertAfterTimestampToSimpleStr(milliseconds int64, t TimeType, n int) string {
	times := time.UnixMilli(milliseconds)
	switch t {
	case YEAR:
		milliseconds = times.AddDate(n, 0, 0).UnixMilli()
	case MONTH:
		milliseconds = times.AddDate(0, n, 0).UnixMilli()
	case DAY:
		milliseconds = times.AddDate(0, 0, n).UnixMilli()
	case HOUR:
		milliseconds = times.Add(time.Duration(n) * time.Hour).UnixMilli()
	case MINUTE:
		milliseconds = times.Add(time.Duration(n) * time.Minute).UnixMilli()
	default:
		panic(fmt.Sprintf("not support TimeType %s", t))
	}
	return ConvertTimestampToSimpleStr(milliseconds)
}

func DiffTimestampBetweenMonths(date1Milliseconds, date2Milliseconds int64) int {
	times1 := time.UnixMilli(date1Milliseconds)
	times2 := time.UnixMilli(date2Milliseconds)
	y1 := times1.Year()
	y2 := times2.Year()
	m1 := int(times1.Month())
	m2 := int(times2.Month())
	d1 := times1.Day()
	d2 := times2.Day()

	yearInterval := y1 - y2
	// 如果 d1的 月-日 小于 d2的 月-日 那么 yearInterval-- 这样就得到了相差的年数
	if m1 < m2 || m1 == m2 && d1 < d2 {
		yearInterval--
	}
	// 获取月数差值
	monthInterval := (m1 + 12) - m2
	if d1 < d2 {
		monthInterval--
	}
	monthInterval %= 12
	month := yearInterval*12 + monthInterval
	return month
}
