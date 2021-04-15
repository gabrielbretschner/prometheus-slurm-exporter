/* Copyright 2020 Joeri Hermans, Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"io/ioutil"
	"os/exec"
	"strings"
	"strconv"
	"regexp"
	"fmt"
)

type GPUsMetrics struct {
	alloc       float64
	idle        float64
	total       float64
	running 	float64
	unused	 	float64
	utilization float64
	node_utilization float64
}

func GPUsGetMetrics() *GPUsMetrics {
	return ParseGPUsMetrics()
}

func ParseAllocatedGPUs() float64 {
	var num_gpus = 0.0

	args := []string{"-a", "-X", "--format=User,Allocgres", "--state=RUNNING", "--noheader", "--parsable2"}
	output := string(Execute("sacct", args))
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				line = strings.Trim(line, "\"")
				fields := strings.Split(line, "|")
				user := fields[0]
				descriptor := strings.TrimPrefix(fields[1], "gpu:")
				fmt.Println(user)
				job_gpus, _ := strconv.ParseFloat(descriptor, 64)
				num_gpus += job_gpus
			}
		}
	}

	return num_gpus
}

func ParseGPUString(descriptor string) float64 {
	descriptor = strings.TrimPrefix(descriptor, "gpu:")
	descriptor = strings.Split(descriptor, "(")[0]
	tentative_gpu := strings.Split(descriptor, ":")
	var num_gpus = 0.0
	if len(tentative_gpu) == 2 {
		gpu, _ := strconv.ParseFloat(tentative_gpu[1], 64)
		num_gpus = gpu
	}else{
		gpu, _ :=  strconv.ParseFloat(descriptor, 64)
		num_gpus = gpu
	}
	return num_gpus
}

func ParseRunningGPUs() float64 {
	var num_gpus = 0.0

	args := []string{"-h", "-o \"%n %T %G\""}
	output := string(Execute("sinfo", args))

	idle := regexp.MustCompile(`^idle`)
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				line = strings.Trim(line, "\"")
				fields := strings.Fields(line)
				if idle.MatchString(fields[1]) == false {
					num_gpus += ParseGPUString(fields[2])
				}
			}
		}
	}

	return num_gpus
}

func ParseTotalGPUs() float64 {
	var num_gpus = 0.0

	args := []string{"-h", "-o \"%n %G\""}
	output := string(Execute("sinfo", args))
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				line = strings.Trim(line, "\"")
				descriptor := strings.Fields(line)[1]
				num_gpus += ParseGPUString(descriptor)			
			}
		}
	}

	return num_gpus
}

func ParseGPUsMetrics() *GPUsMetrics {
	var gm GPUsMetrics
	total_gpus := ParseTotalGPUs()
	allocated_gpus := ParseAllocatedGPUs()
	running_gpus := ParseRunningGPUs()
	gm.alloc = allocated_gpus
	gm.running = running_gpus
	gm.idle = total_gpus - allocated_gpus
	gm.unused = running_gpus - allocated_gpus
	gm.node_utilization = allocated_gpus / running_gpus
	gm.total = total_gpus
	gm.utilization = allocated_gpus / total_gpus
	return &gm
}

// Execute the sinfo command and return its output
func Execute(command string, arguments []string) []byte {
	cmd := exec.Command(command, arguments...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	return &GPUsCollector{
		alloc: prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", nil, nil),
		idle:  prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", nil, nil),
		total: prometheus.NewDesc("slurm_gpus_total", "Total GPUs", nil, nil),
		running: prometheus.NewDesc("slurm_gpus_running", "Running GPUs", nil, nil),
		unused: prometheus.NewDesc("slurm_gpus_unused", "Idle running GPUs", nil, nil),
		utilization: prometheus.NewDesc("slurm_gpus_utilization", "Total GPU utilization", nil, nil),
		node_utilization: prometheus.NewDesc("slurm_gpus_node_utilization", "Total running GPU nodes utilization", nil, nil),
	}
}

type GPUsCollector struct {
	alloc       *prometheus.Desc
	idle        *prometheus.Desc
	total       *prometheus.Desc
	running     *prometheus.Desc
	unused      *prometheus.Desc
	utilization *prometheus.Desc
	node_utilization *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.running
	ch <- cc.unused
	ch <- cc.utilization
	ch <- cc.node_utilization
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	cm := GPUsGetMetrics()
	ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc)
	ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle)
	ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total)
	ch <- prometheus.MustNewConstMetric(cc.running, prometheus.GaugeValue, cm.running)
	ch <- prometheus.MustNewConstMetric(cc.unused, prometheus.GaugeValue, cm.unused)
	ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, cm.utilization)
	ch <- prometheus.MustNewConstMetric(cc.node_utilization, prometheus.GaugeValue, cm.node_utilization)
}
