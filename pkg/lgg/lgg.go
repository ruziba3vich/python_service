package lgg

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

type Logger struct {
	*logrus.Logger
}

func NewLogger() (*Logger, error) {
	log := logrus.New()

	file, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	log.SetOutput(file)

	log.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})

	log.SetLevel(logrus.InfoLevel)

	return &Logger{log}, nil
}

func (l *Logger) Debug(msg string, fields ...map[string]any) {
	l.addFields(l.Logger.Debug, msg, fields...)
}

func (l *Logger) Info(msg string, fields ...map[string]any) {
	if len(fields) > 0 {
		l.WithFields(fields[0]).Info(msg)
	} else {
		l.Logger.Info(msg)
	}
}

func (l *Logger) Warn(msg string, fields ...map[string]any) {
	l.addFields(l.Logger.Warn, msg, fields...)
}

func (l *Logger) Error(msg string, fields ...map[string]any) {
	l.addFields(l.Logger.Error, msg, fields...)
}

func (l *Logger) addFields(logFunc func(...any), msg string, fields ...map[string]any) {
	if len(fields) > 0 {
		entry := l.WithFields(fields[0])
		entry.Log(logrus.InfoLevel, msg)
	} else {
		logFunc(msg)
	}
}
