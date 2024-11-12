package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
)

type cmdresult struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

func homepage(write http.ResponseWriter, _ *http.Request) {
	fmt.Fprintf(write, "Go Home Simple REST API Server")
}

func getdate(write http.ResponseWriter, _ *http.Request) {
	result := cmdresult{}
	out, err := exec.Command("date").Output()
	if err == nil {
		result.Success = true
		result.Message = strings.Trim("The date is "+string(out), "\n")
	}
	json.NewEncoder(write).Encode(result)

}

func main() {
	http.HandleFunc("/", homepage)
	http.HandleFunc("/api/v1/getdate", getdate)
	err := http.ListenAndServe(":4000", nil)
	if err != nil {
		fmt.Println("Failed to start server:", err)
		os.Exit(1)
	}
}
