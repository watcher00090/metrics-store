package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
)

// Always has a slash at the end
var datapath string

var latestValues map[string]string

func appendSeparatorIfNecessary(path string) string {
	var lastChar = path[len(datapath)-1]
	if lastChar != filepath.Separator {
		path += string(filepath.Separator)
	}
	return path
}

const response = "getData() method returned successfully"

func getCustomers(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, "hi")
}

func setDataPath(c *gin.Context) {
	path := c.Query("datapath")
	if path == "" {
		c.String(http.StatusBadRequest, "ERROR: Please supply the 'datapath' query parameter and try again.")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		c.String(http.StatusInternalServerError, "ERROR: Could not convert the datapath to an absolute path. Error: "+err.Error())
	}
	datapath = appendSeparatorIfNecessary(absPath)
	c.String(http.StatusOK, "SUCCESS: The datapath has been set to "+datapath)
}

func addData(c *gin.Context) {
	topic := c.Query("topic")
	var data interface{}
	json.Unmarshal([]byte(c.Query("data")), data)

	var f *os.File

	f, err := os.OpenFile(datapath+topic+"-topic-metrics-data.txt", os.O_RDWR, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// handle the case where the backing file doesn't exist
			c.String(http.StatusInternalServerError, "ERROR: The topic '"+topic+"' does not exist. Please create the topic and try again. ")
		}
		if errors.Is(err, os.ErrPermission) {
			c.String(http.StatusInternalServerError, "ERROR: The backing file has invalid permissions. Please give the user running the metrics processor RW permissions over the backing file and try again.")
		}
		panic("ERROR: " + err.Error())
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic("ERROR: Unable to marshal the data into a JSON string, exiting.")
	}
	bytes = append(bytes, '\n')

	_, err = f.Write(bytes)
	if err != nil {
		c.String(http.StatusInternalServerError, "ERROR: Unable to add the data to the topic. The error was: "+err.Error())
	}
	c.String(http.StatusAccepted, "SUCCESS: The data was successfully pushed to the topic!")
}

func makeTopic(c *gin.Context) {
	name := c.Param("name")
	_, err := os.Create(datapath + name + "-topic-metrics-data.txt")
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			c.String(http.StatusInternalServerError, "ERROR: The topic already exists.")
		}
		if errors.Is(err, os.ErrPermission) {
			c.String(http.StatusInternalServerError, "ERROR: Unable to create the backing file due to file permissions issues. Please check the permissions of the datapath directory and try again.")
		}
		c.String(http.StatusInternalServerError, "ERROR: Unable to create the file backing the topic.")
	}
}

func getData(c *gin.Context) {
	topic := c.Param("topic")
	_, err := os.OpenFile(datapath+topic+"-topic-metrics-data.txt", os.O_RDWR, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// handle the case where the backing file doesn't exist
			c.String(http.StatusInternalServerError, "ERROR: The topic '"+topic+"' does not exist. Please create the topic and try again. ")
		}
		if errors.Is(err, os.ErrPermission) {
			c.String(http.StatusInternalServerError, "ERROR: The backing file has invalid permissions. Please give the user running the metrics processor RW permissions over the backing file and try again.")
		}
		panic("ERROR: " + err.Error())
	}
	// Return the contents of the file backing the topic
	c.File(datapath + topic + "-topic-metrics-data.txt")
}

func getLatestValue(c *gin.Context) {
	topic := c.Param("topic")
	f, err := os.OpenFile(datapath+topic+"-topic-metrics-data.txt", os.O_RDWR, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// handle the case where the backing file doesn't exist
			c.String(http.StatusInternalServerError, "ERROR: The topic '"+topic+"' does not exist. Please create the topic and try again. ")
		}
		panic(err.Error())
	}

	fileInfo, _ := os.Stat(datapath + topic + "-topic-metrics-data.txt")
	if fileInfo.Size() == 0 {
		c.String(http.StatusInternalServerError, "ERROR: No data currently in the topic!")
	}

	// Get the last line of the backing file
	var k int64 = 1 // start at 1 because the last character will be \n
	var offset int64
	offset, err = f.Seek(k, 2)
	var currChar []byte
	for f.Read(currChar); currChar[0] != 10; { // 10 is \n in ASCII
		k++
		offset, err = f.Seek(k, 2)
		if err != nil {
			panic("ERROR: " + err.Error())
		}
	}
	// Go forward by one character
	offset, err = f.Seek(offset, 1)

	// Extract the last line
	lastLineBuffer := make([]byte, k-1)

	// Read the last line
	_, err = f.Read(lastLineBuffer)
	if err != nil {
		panic(err)
	}
	// Return the data
	c.String(http.StatusOK, string(lastLineBuffer))
}

func listTopics(c *gin.Context) {

}

// If the METRICS_PROCESSOR_DATAPATH environment variable isn't set, return 404 until the user calls 'PUT /datapath?path=<PATH>'
// If it is set, use it as the datapath
func main() {
	// gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	path, isSet := os.LookupEnv("METRICS_PROCESSOR_DATAPATH")
	if !isSet {
		fmt.Println("METRICS_PROCESSOR_DATAPATH variable not set, waiting for the user to call /configure")
	} else {
		fmt.Println("Using the METRICS_PROCESSOR_DATAPATH environment variable as the datapath")
		absPath, err := filepath.Abs(path)
		if err != nil {
			panic("ERROR: Could not convert the datapath to an absolute path. Error: " + err.Error())
		}
		datapath = appendSeparatorIfNecessary(absPath)
	}

	router.Handle("GET", "/configure", setDataPath)
	router.Handle("GET", "/data/:topic", getData)
	router.Handle("GET", "/customers", getCustomers)
	router.Handle("PUT", "/data", addData)

	router.Run(":8080")
}
