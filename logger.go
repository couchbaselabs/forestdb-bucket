package forestbucket

import "log"

// Implementation of the goforestdb Logger interface that passes everything
// through to the standard log package

type logger struct{}

func (l logger) Warnf(format string, v ...interface{}) {
	log.Printf(format, v)
}

func (l logger) Errorf(format string, v ...interface{}) {
	log.Printf(format, v)
}

func (l logger) Fatalf(format string, v ...interface{}) {
	log.Printf(format, v)
}

func (l logger) Infof(format string, v ...interface{}) {
	log.Printf(format, v)
}

func (l logger) Debugf(format string, v ...interface{}) {
	log.Printf(format, v)
}

func (l logger) Tracef(format string, v ...interface{}) {
	log.Printf(format, v)
}
