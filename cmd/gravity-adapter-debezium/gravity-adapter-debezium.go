package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	app "github.com/BrobridgeOrg/gravity-adapter-debezium/pkg/app/instance"
)

func init() {

	debugLevel := log.InfoLevel
	switch os.Getenv("GRAVITY_DEBUG") {
	case log.TraceLevel.String():
		debugLevel = log.TraceLevel
	case log.DebugLevel.String():
		debugLevel = log.DebugLevel
	case log.ErrorLevel.String():
		debugLevel = log.ErrorLevel
	}

	log.SetLevel(debugLevel)

	fmt.Printf("Debug level is set to \"%s\"\n", debugLevel.String())

	// From the environment
	viper.SetEnvPrefix("GRAVITY_ADAPTER_DEBEZIUM")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// From config file
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.AddConfigPath("./configs")

	if err := viper.ReadInConfig(); err != nil {
		log.Warn("No configuration file was loaded")
	}

	runtime.GOMAXPROCS(8)
	/*
		return
		go func() {

			f, err := os.Create("cpu-profile.prof")
			if err != nil {
				log.Fatal(err)
			}

			pprof.StartCPUProfile(f)

			sig := make(chan os.Signal, 1)
			signal.Notify(sig, os.Interrupt, os.Kill)
			<-sig
			pprof.StopCPUProfile()

			os.Exit(0)
		}()
	*/
}

func main() {

	// Initializing application
	a := app.NewAppInstance()

	err := a.Init()
	if err != nil {
		log.Fatal(err)
		return
	}

	// Starting application
	err = a.Run()
	if err != nil {
		log.Fatal(err)
		return
	}
}
