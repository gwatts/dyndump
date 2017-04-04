// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package cmd

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/jawher/mow.cli"
)

type action interface {
	init() error
	newProgressBar() (bar *pb.ProgressBar)
	updateProgress(bar *pb.ProgressBar)
	start(termWriter io.Writer, logWriter *log.Logger) (doneChan chan error, err error)
	abort()
	printFinalStats(w io.Writer)
}

type progressLogger interface {
	logProgress(logger *log.Logger)
}

// actionRunner handles running an action which may take a while to complete
// providing progress bars and signal handling.
func actionRunner(cmd *cli.Cmd, action action) func() {
	cmd.Spec = "[--silent] [--no-progress] [--log] " + cmd.Spec
	silent := cmd.Bool(cli.BoolOpt{
		Name:   "silent",
		Value:  false,
		Desc:   "Set to true to disable all non-error and non-log output",
		EnvVar: "SILENT",
	})
	noProgress := cmd.Bool(cli.BoolOpt{
		Name:   "no-progress",
		Value:  false,
		Desc:   "Set to true to disable the progress bar",
		EnvVar: "NO_PROGRESS",
	})
	logTarget := cmd.String(cli.StringOpt{
		Name:   "log",
		Value:  "",
		Desc:   "Set to a filename or --log=- for stdout; defaults to no log output",
		EnvVar: "LOG_TARGET",
	})

	return func() {
		var termWriter io.Writer = os.Stderr
		var logger *log.Logger
		var progressTicker <-chan time.Time
		var logTicker <-chan time.Time

		nullLogger := log.New(ioutil.Discard, "", log.LstdFlags)

		switch target := *logTarget; target {
		case "-":
			logger = log.New(os.Stdout, "", log.LstdFlags)
		case "":
			logger = nullLogger
		default:
			f, err := os.OpenFile(target, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
			if err != nil {
				fail("could not open logfile for write: %s", err)
			}
			defer f.Close()
			logger = log.New(f, "", log.LstdFlags)
		}

		if logger != nullLogger {
			if _, ok := action.(progressLogger); ok {
				logTicker = time.Tick(logFrequency)
			}
		}

		if *silent {
			termWriter = ioutil.Discard
		}

		if err := action.init(); err != nil {
			fail("Initialization failed: %v", err)
		}

		done, err := action.start(termWriter, logger)
		if err != nil {
			fail("Startup failed: %v", err)
		}

		var bar *pb.ProgressBar
		if !*silent && !*noProgress {
			progressTicker = time.Tick(statsFrequency)
			bar = action.newProgressBar()
			if bar != nil {
				bar.Output = os.Stderr
				bar.ShowSpeed = true
				bar.ManualUpdate = true
				bar.SetMaxWidth(78)
				bar.Start()
				bar.Update()
			}
		}

		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT)

	LOOP:
		for {
			select {
			case <-progressTicker:
				action.updateProgress(bar)
				bar.Update()

			case <-logTicker:
				action.(progressLogger).logProgress(logger)

			case <-sigchan:
				if bar != nil {
					bar.Finish()
					bar = nil
				}
				fmt.Fprintf(termWriter, "\nAborting..")
				action.abort()
				<-done
				fmt.Fprintf(termWriter, "Aborted.\n")
				break LOOP

			case err := <-done:
				if bar != nil {
					bar.Finish()
					bar = nil
				}
				if err != nil {
					fail("Processing failed: %v", err)
				}
				break LOOP
			}
		}

		if !*silent {
			action.printFinalStats(termWriter)
		}
	}
}
