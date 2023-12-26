package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gosimple/slug"

	"golang.org/x/net/html"
)

const (
	apiUrl          = "https://www.titck.gov.tr/getkubktviewdatatable"
	payloadTpl      = "draw=1&columns%5B0%5D%5Bdata%5D=name&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=element&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=firmName&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=confirmationDateKub&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=confirmationDateKt&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=documentPathKub&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=documentPathKt&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D=asc&start={{START}}&length={{LENGTH}}&search%5Bvalue%5D=&search%5Bregex%5D=false&_token=NVjcfwHO5a1e4g33cv8u1np5Qz7OMztA09b3eVJ1"
	pdfDownloadPath = "./docs/pdf/%s"
)

type Scraper struct {
	chunks      int
	limit       int
	workerCount int
	cookie      string
	wg          sync.WaitGroup
}

func New(options ...func(*Scraper)) *Scraper {
	s := &Scraper{}
	for _, o := range options {
		o(s)
	}
	return s
}

func WithChunks(chunks int) func(*Scraper) {
	return func(s *Scraper) {
		s.chunks = chunks
	}
}

func WithLimit(limit int) func(*Scraper) {
	return func(s *Scraper) {
		s.limit = limit
	}
}

func WithWorkerCount(workerCount int) func(*Scraper) {
	return func(s *Scraper) {
		s.workerCount = workerCount
	}
}

func WithCookie(cookie string) func(*Scraper) {
	return func(s *Scraper) {
		s.cookie = cookie
	}
}

type (
	apiResponse struct {
		Draw            int `json:"draw"`
		RecordsTotal    int `json:"recordsTotal"`
		RecordsFiltered int `json:"recordsFiltered"`
		Data            []struct {
			Name                string `json:"name"`
			Element             string `json:"element"`
			FirmName            string `json:"firmName"`
			ConfirmationDateKub string `json:"confirmationDateKub"`
			ConfirmationDateKt  string `json:"confirmationDateKt"`
			DocumentPathKub     string `json:"documentPathKub"`
			DocumentPathKt      string `json:"documentPathKt"`
		} `json:"data"`
	}
	pdfData struct {
		DrugName  string
		FileName  string
		RemoteUrl string
		LocalPath string
	}
)

func (s *Scraper) Run() {
	ctx := context.TODO()
	responseChan := s.consumeApi(ctx)
	pdfChan := s.readApiResponse(responseChan)

	for i := 0; i < s.workerCount; i++ {
		s.wg.Add(1)
		go s.download(pdfChan)
	}

	s.wg.Wait()
}

func (s *Scraper) consumeApi(ctx context.Context) <-chan *apiResponse {
	apiResponseChan := make(chan *apiResponse)

	go func() {
		defer close(apiResponseChan)
		currentIndex := 0
		totalItems := 0

		for {
			data, err := s.getData(currentIndex, s.chunks)
			if err != nil {
				slog.Error("getData error", "currentIndex", currentIndex, "error", err)
				continue
			}

			totalItems = data.RecordsTotal
			currentIndex += s.chunks

			apiResponseChan <- data

			if currentIndex >= totalItems || currentIndex >= s.limit {
				break
			}
		}
	}()

	return apiResponseChan
}

func (s *Scraper) readApiResponse(apiResponseChan <-chan *apiResponse) <-chan *pdfData {
	pdfDataChan := make(chan *pdfData)

	go func() {
		defer close(pdfDataChan)
		for apiResponse := range apiResponseChan {
			for _, item := range apiResponse.Data {
				remoteUrl, err := extractPDFLinkFromHTML(item.DocumentPathKub)
				if err != nil {
					slog.Error("extractPDFFromHTML error", "error", err)
					continue
				}
				trimmedName := strings.Trim(item.Name, " ")
				pdfDataChan <- &pdfData{
					DrugName:  trimmedName,
					FileName:  slug.Make(trimmedName) + ".pdf",
					RemoteUrl: remoteUrl,
				}
			}
		}
	}()

	return pdfDataChan
}

func (s *Scraper) download(pdfChan <-chan *pdfData) {
	defer s.wg.Done()

	for pdfData := range pdfChan {
		response, err := http.Get(pdfData.RemoteUrl)
		if err != nil {
			slog.Error("http.Get error", "pdfLink", pdfData.RemoteUrl, "error", err)
			continue
		}

		downloadPath := fmt.Sprintf(pdfDownloadPath, pdfData.FileName)
		if _, err := os.Stat(downloadPath); err == nil {
			pdfData.FileName = fmt.Sprintf("%s-%d.pdf", slug.Make(pdfData.DrugName), response.ContentLength)
			downloadPath = fmt.Sprintf(pdfDownloadPath, pdfData.FileName)
		}

		file, err := os.Create(downloadPath)
		if err != nil {
			slog.Error("os.Create error", "pdfLink", pdfData.RemoteUrl, "error", err)
			continue
		}

		_, err = io.Copy(file, response.Body)
		if err != nil {
			slog.Error("os.Create error", "pdfLink", pdfData.RemoteUrl, "error", err)
			continue
		}
		response.Body.Close()
		file.Close()

		slog.Info("downloaded the file", "pdfLink", pdfData.RemoteUrl)
	}
}

func (s *Scraper) getData(start, length int) (*apiResponse, error) {
	client := &http.Client{}
	payload := strings.ReplaceAll(payloadTpl, "{{START}}", fmt.Sprintf("%d", start))
	payload = strings.ReplaceAll(payload, "{{LENGTH}}", fmt.Sprintf("%d", length))

	req, err := http.NewRequest("POST", apiUrl, strings.NewReader(payload))

	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Add("Referer", "https://www.titck.gov.tr/kubkt")
	req.Header.Add("X-Requested-With", "XMLHttpRequest")
	req.Header.Add("Cookie", s.cookie)

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	response := &apiResponse{}
	err = json.Unmarshal(body, response)
	if err != nil {
		return nil, err
	}

	slog.Info("retrieved data", "start", start, "length", length)
	return response, nil
}

func extractPDFLinkFromHTML(htmlContent string) (string, error) {
	reader := strings.NewReader(htmlContent)

	tokenizer := html.NewTokenizer(reader)

	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			return "", fmt.Errorf("error parsing HTML")
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()

			if token.Data == "a" {
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						return attr.Val, nil
					}
				}
			}
		}
	}
}
