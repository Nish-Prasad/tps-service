package service

import (
	"os"
	"sync"
	"time"
)

type LogEntry struct {
	Timestamp time.Time
	RequestID string
	Message   string
}

var (
	logsByRequestID = make(map[string][]LogEntry)
	mu              sync.Mutex
	logFile         *os.File
)

func InitializeLogger() {
	var err error
	logFile, err = os.OpenFile("grouped_logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
}

// Log adds a new log entry
func Log(requestID, message string) {
	mu.Lock()
	defer mu.Unlock()

	entry := LogEntry{
		Timestamp: time.Now(),
		RequestID: requestID,
		Message:   message,
	}

	logsByRequestID[requestID] = append(logsByRequestID[requestID], entry)

	// Write immediately to file too
	logLine := "[" + entry.Timestamp.Format("2006-01-02 15:04:05.000") + "] " +
		"RequestID: " + requestID + " -> " + message + "\n"
	logFile.WriteString(logLine)
	// fmt.Printf("\n[%s][%s]\n",requestID, message)
}

// GroupedLogs returns all logs grouped by RequestID
func GroupedLogs() map[string][]LogEntry {
	mu.Lock()
	defer mu.Unlock()

	// Copy to avoid race conditions
	copyMap := make(map[string][]LogEntry)
	for reqID, entries := range logsByRequestID {
		copyMap[reqID] = append([]LogEntry(nil), entries...)
	}
	return copyMap
}

// Close should be called before program exits
func Close() {
	logFile.Close()
}