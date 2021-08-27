package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	regex "regexp"
	"strings"

	"github.com/gin-gonic/gin"
)

// Always has a slash at the end
var datapath string

var latestValues map[string]string

func appendSeparatorIfNecessary(path string) string {
	var lastChar = path[len(path)-1]
	if lastChar != filepath.Separator {
		path += string(filepath.Separator)
	}
	return path
}

const response = "getData() method returned successfully"

func rootPath(c *gin.Context) {
	c.HTML(http.StatusOK, "index.html", gin.H{})
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
	json.Unmarshal([]byte(c.Query("data")), &data)

	var f *os.File

	f, err := os.OpenFile(datapath+topic+".topic.metrics.data.txt", os.O_RDWR|os.O_APPEND, 0644)
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
	c.String(http.StatusOK, "SUCCESS: The data was successfully pushed to the topic!")
}

func makeTopic(c *gin.Context) {
	name := c.Query("topic")
	fmt.Printf("Creating topic %s...\n", name)

	if _, err := os.Stat(datapath + name + ".topic.metrics.data.txt"); errors.Is(err, os.ErrNotExist) {
		// file does not exist, so create it
		_, err := os.Create(datapath + name + ".topic.metrics.data.txt")
		if err != nil {
			if errors.Is(err, os.ErrPermission) {
				c.String(http.StatusInternalServerError, "ERROR: Unable to create the backing file due to file permissions issues. Please check the permissions of the datapath directory and try again.")
			}
			c.String(http.StatusInternalServerError, "ERROR: Unable to create the file backing the topic.")
		}
		c.String(http.StatusOK, "Success: Topic %s created successfully", name)
	} else {
		// file exists
		c.String(http.StatusInternalServerError, "ERROR: The topic already exists.")
	}
}

func getData(c *gin.Context) {
	topic := c.Query("topic")
	_, err := os.OpenFile(datapath+topic+".topic.metrics.data.txt", os.O_RDWR, 0644)
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
	c.File(datapath + topic + ".topic.metrics.data.txt")
}

func getLatestValue(c *gin.Context) {
	topic := c.Query("topic")
	f, err := os.OpenFile(datapath+topic+".topic.metrics.data.txt", os.O_RDWR, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// handle the case where the backing file doesn't exist
			c.String(http.StatusInternalServerError, "ERROR: The topic '"+topic+"' does not exist. Please create the topic and try again. ")
		}
		panic(err.Error())
	}

	fileInfo, _ := os.Stat(datapath + topic + ".topic.metrics.data.txt")
	if fileInfo.Size() == 0 {
		c.String(http.StatusInternalServerError, "ERROR: No data currently in the topic!")
	}

	// Get the last line of the backing file
	// var k int64 = -1 // start at 1 because the last character will be \n
	// var offset int64

	var k int64 = -2
	var offset int64
	var lineLen int = 0

	var currChar []byte = make([]byte, 1)
	offset, err = f.Seek(k, 2)
	if err != nil {
		panic("ERROR: " + err.Error())
	}
	_, err = f.Read(currChar)

	for currChar[0] != '\n' && offset != 0 {
		fmt.Printf("offset = %d, k = %d, currChar = %c\n", offset, k, currChar)
		k--
		lineLen++

		offset, err = f.Seek(k, 2)
		fmt.Printf("offset = %d\n", offset)

		if err != nil {
			panic("ERROR: " + err.Error())
		}
		_, err = f.Read(currChar)
		if err != nil {
			panic("ERROR: " + err.Error())
		}
	}
	if currChar[0] == '\n' {
		// Go forward by one character
		_, err = f.Seek(k+1, 2)
	} else { // offset = 0
		lineLen++
		_, err = f.Seek(0, 0)
	}

	// Extract the last line
	lastLineBuffer := make([]byte, lineLen)

	// Read the last line
	_, err = f.Read(lastLineBuffer)
	if err != nil {
		panic(err)
	}

	// Return the data
	c.String(http.StatusOK, strings.Trim(string(lastLineBuffer), "\t \n"))
}

func listTopics(c *gin.Context) {
	var entries []os.DirEntry
	var err error
	entries, err = os.ReadDir(datapath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// handle the case where the backing file doesn't exist
			c.String(http.StatusInternalServerError, "ERROR: Unable to read from the directory %s. Please ensure that the directory has the appropriate permissions and try again.", datapath)
		}
		panic(err.Error())
	}
	re := regex.MustCompile("([^.]*)\\.topic\\.metrics\\.data\\.txt*")
	var topics string = ""
	for i := 0; i < len(entries); i++ {
		if matches := re.FindStringSubmatch(entries[i].Name()); matches != nil {
			topics += matches[1]
		}
		if i != len(entries)-1 {
			topics += "\n"
		}
	}
	c.String(http.StatusOK, topics)
}

// If the METRICS_PROCESSOR_DATAPATH environment variable isn't set, return 404 until the user calls 'PUT /datapath?path=<PATH>'
// If it is set, use it as the datapath
func main() {
	// gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.LoadHTMLFiles("index.html")

	path, isSet := os.LookupEnv("METRICS_STORE_DATAPATH")
	if !isSet {
		fmt.Println("METRICS_STORE_DATAPATH variable not set, waiting for the user to call /configure")
	} else {
		fmt.Println("Using the METRICS_STORE_DATAPATH environment variable as the datapath")
		absPath, err := filepath.Abs(path)
		if err != nil {
			panic("ERROR: Could not convert the datapath to an absolute path. Error: " + err.Error())
		}
		datapath = appendSeparatorIfNecessary(absPath)
	}

	router.Handle("GET", "/configure", setDataPath)
	router.Handle("GET", "/data", getData)
	router.Handle("GET", "/", rootPath)
	router.Handle("PUT", "/put", addData)
	router.Handle("PUT", "/create", makeTopic)
	router.Handle("GET", "/latest", getLatestValue)
	router.Handle("GET", "/topics", listTopics)

	router.Run(":8080")
}
