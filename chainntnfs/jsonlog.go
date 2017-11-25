package chainntnfs

import (
	"github.com/rs/zerolog"
	"os"
)

var JsonLog zerolog.Logger

func init() {
	f, err := os.OpenFile("testlogfile", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		println("error opening file: %v", err)
	}
	defer f.Close()

	JsonLog = zerolog.New(f).With().Str("src", "lnd").Logger()
}
