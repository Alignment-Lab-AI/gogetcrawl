package common

import (
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/corpix/uarand"
	"github.com/valyala/fasthttp"
)

var (
	Status503Error = errors.New("Server returned 503 status response")
	Status500Error = errors.New("Server returned 500 status response. (Slow down)")
)

// WebArchive and Common Crawl (index.commoncrawl.org) CDX API Response structure from
type CdxResponse struct {
	Urlkey       string `json:"urlkey,omitempty"`
	Timestamp    string `json:"timestamp,omitempty"`
	Charset      string `json:"charset,omitempty"`
	MimeType     string `json:"mime,omitempty"`
	Languages    string `json:"languages,omitempty"`
	MimeDetected string `json:"mimedetected,omitempty"`
	Digest       string `json:"digest,omitempty"`
	Offset       string `json:"offset,omitempty"`
	Original     string `json:"url,omitempty"` // Original URL
	Length       string `json:"length,omitempty"`
	StatusCode   string `json:"status,omitempty"`
	Filename     string `json:"filename,omitempty"`
	Source       Source
}

// Source of web archive data
type Source interface {
	Name() string
	ParseResponse(resp []byte) ([]*CdxResponse, error)
	GetNumPages(url string) (int, error)
	GetPages(config RequestConfig) ([]*CdxResponse, error)
	FetchPages(config RequestConfig, results chan []*CdxResponse, errors chan error)
	GetFile(*CdxResponse) ([]byte, error)
}

type RequestConfig struct {
	URL            string    // Url to parse
	Filters        []string  // Extenstion to search
	Limit          uint      // Max number of results per page
	CollapseColumn string    // Which column to use to collapse results
	SinglePage     bool      // Get results only from 1st page (mostly used for tests)
	FromDate       time.Time // Filter results from Date
	ToDate         time.Time // Filter results to Date
}

// GetUrlFromConfig ... Compose URL with CDX server request parameters
func (config RequestConfig) GetUrl(serverURL string, page int) string {
	reqURL := fmt.Sprintf("%v?url=%v&output=json", serverURL, config.URL)

	if config.Limit != 0 {
		reqURL = fmt.Sprintf("%v&limit=%v", reqURL, config.Limit)
	}

	if config.CollapseColumn != "" {
		reqURL = fmt.Sprintf("%v&collapse=%v", reqURL, config.CollapseColumn)
	}

	for _, filter := range config.Filters {
		if filter != "" {
			reqURL = fmt.Sprintf("%v&filter=%v", reqURL, filter)
		}
	}

	if !config.FromDate.IsZero() {
		reqURL = fmt.Sprintf("%v&from=%v", reqURL, config.FromDate.Format("20060102"))
	}

	if !config.ToDate.IsZero() {
		reqURL = fmt.Sprintf("%v&to=%v", reqURL, config.ToDate.Format("20060102"))
	}

	if !config.SinglePage {
		reqURL = fmt.Sprintf("%v&page=%v", reqURL, page)
	}
	return reqURL
}

func DoRequest(url string, timeout int, headers map[string]string) ([]byte, error) {
	timeoutDuration := time.Second * time.Duration(timeout)

	req := fasthttp.AcquireRequest()
	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.Header.Set(fasthttp.HeaderUserAgent, uarand.GetRandom())
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	client := &fasthttp.Client{}
	client.ReadTimeout = timeoutDuration
	err := client.DoTimeout(req, resp, timeoutDuration)
	if err != nil {
		return nil, fmt.Errorf("[GetRequest] Error making request: %v", err)
	}

	switch resp.StatusCode() {
	case 500:
		return nil, Status500Error
	case 503:
		return resp.Body(), Status503Error
	}

	if len(resp.Body()) > 0 {
		return resp.Body(), nil
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("[GetRequest] Got %v status response", resp.StatusCode())
	}

	if resp.Body() == nil {
		return nil, fmt.Errorf("[GetRequest] Response body is empty")
	}

	return resp.Body(), nil
}

// Get ... Performs HTTP GET request and returns response bytes
func Get(url string, timeout int, maxRetries int) ([]byte, error) {
	client := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	var resp *http.Response
	var err error

	for i := 0; i < maxRetries; i++ {
		log.Printf("GET [t=%v] [r=%v]: %v", timeout, maxRetries, url)

		resp, err = client.Get(url)
		if err == nil && resp.StatusCode == 200 {
			break
		}
		log.Printf("Attempt %d failed: %v", i+1, err)
		time.Sleep(time.Second * time.Duration(i+1))
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// Save data using file fullpath
func SaveFile(data []byte, path string) error {
	err := os.WriteFile(path, data, 0o644)
	if err != nil {
		return err
	}

	return nil
}

// Save files from CDX Response channel into output directory
func SaveFiles(results <-chan []*CdxResponse, outputDir string, errors chan error, downloadRate float32) {
	for resBatch := range results {
		for _, res := range resBatch {
			data, err := res.Source.GetFile(res)
			if err != nil {
				errors <- err
				continue
			}

			exts, err := mime.ExtensionsByType(res.MimeType)
			if err != nil || len(exts) == 0 {
				errors <- fmt.Errorf("Cannot get extension from file")
				continue
			}

			filename := fmt.Sprintf("%v-%v-%v%v", res.Original, res.Timestamp, res.Source.Name(), exts[0])
			fullPath := filepath.Join(outputDir, url.QueryEscape(filename))

			if err := SaveFile(data, fullPath); err != nil {
				errors <- err
			}

			time.Sleep(time.Duration(downloadRate * float32(time.Second)))
		}
	}
}
