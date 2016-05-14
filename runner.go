// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package main

import (
	"fmt"
	"io"
	"io/ioutil"
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
	start(w io.Writer) (doneChan chan error, err error)
	abort()
	printFinalStats(w io.Writer)
}

// actionRunner handles running an action which may take a while to complete
// providing progress bars and signal handling.
func actionRunner(cmd *cli.Cmd, action action) func() {
	cmd.Spec = "[--silent] [--no-progress] " + cmd.Spec
	silent := cmd.BoolOpt("silent", false, "Set to true to disable all non-error output")
	noProgress := cmd.BoolOpt("no-progress", false, "Set to true to disable the progress bar")

	return func() {
		var infoWriter io.Writer = os.Stderr
		var ticker <-chan time.Time

		if err := action.init(); err != nil {
			fail("Initialization failed: %v", err)
		}

		done, err := action.start(infoWriter)
		if err != nil {
			fail("Startup failed: %v", err)
		}

		var bar *pb.ProgressBar
		if !*silent && !*noProgress {
			ticker = time.Tick(statsFrequency)
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
		if *silent {
			infoWriter = ioutil.Discard
		}

		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT)

	LOOP:
		for {
			select {
			case <-ticker:
				action.updateProgress(bar)
				bar.Update()

			case <-sigchan:
				bar.Finish()
				fmt.Fprintf(os.Stderr, "\nAborting..")
				action.abort()
				<-done
				fmt.Fprintf(os.Stderr, "Aborted.\n")
				break LOOP

			case err := <-done:
				if err != nil {
					fail("Processing failed: %v", err)
				}
				break LOOP
			}
		}
		if bar != nil {
			bar.Finish()
		}

		if !*silent {
			action.printFinalStats(infoWriter)
		}
	}
}
