package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly"
	"github.com/gocolly/colly/debug"
	"github.com/gocolly/colly/extensions"
	"github.com/gocolly/colly/proxy"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type WineInfo struct {
	URL            string     `bson:"url"`
	ID             string     `bson:"id"`
	Title          string     `bson:"title"`
	Vineyard       string     `bson:"vineyard,omitempty"`
	Year           int        `bson:"year"`
	Rating         RatingInfo `bson:"rating,inline"`
	Country        string     `bson:"country,omitempty"`
	Appellation    string     `bson:"appellation,omitempty"`
	GeoIdentifiers []string   `bson:"GeoIdentifiers"`
	GrapeVarietal  string     `bson:"grape_varietal,omitempty"`
	Type           string     `bson:"type_tags,omitempty"`
	Producer       string     `bson:"producer,omitempty"`
	Designation    string     `bson:"designation,omitempty"`
	ImgSource      []string   `bson:"img_src,omitempty"`
}

type RatingInfo struct {
	AverageRating float64 `bson:"average_rating"`
	MedianRating  float64 `bson:"median_rating"`
	NumRatings    int     `bson:"num_ratings"`
}

var (
	urlBase     = "https://www.cellartracker.com/classic/wine.asp?PrinterFriendly=true&iWine=%d&Label=%d"
	wineMap     = make(map[string]WineInfo)
	mapMutex    = &sync.Mutex{}
	toRescrape  = []int{}
	scrapedVals = []int{}
	invalidVals = []int{}
)

// Define a slice of user agents
var userAgents = []string{

	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36", // my current UA
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux i686; rv:109.0) Gecko/20100101 Firefox/121.0",
	"Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/121.0",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
}

// UserAgentIndex to track the current User Agentnnode
var userAgentIndex int

func main() {

	// CONNECT TO ATLAS DATABASE https://cloud.mongodb.com/v2/65da774c19ccda13b572907b#/clusters

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// have URI saved as an environment variable
	opts := options.Client().ApplyURI(os.Getenv("ATLAS_URI"))

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	// JUST TO BE SURE OUR CLUSTER IS REALLY CONNECTED... Send a ping to confirm a successful connection
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Err(); err != nil {
		panic(err)
	}
	fmt.Println("Pinged your deployment. connected to MongoDB!")

	// CONNECTION TO DATABASE SUCCESSFUL

	// START INITIALIZING COLLY
	c := colly.NewCollector(
		colly.AllowURLRevisit(),
		colly.Async(false),
		colly.Debugger(&debug.LogDebugger{}),
	)

	// Rotate two socks5 proxies
	rp, err := proxy.RoundRobinProxySwitcher("http://198.199.8.11:3128", "socks5://206.206.71.249:5889", "socks5://184.181.217.210:4145", "socks5://184.181.217.201:4145", "socks5://166.62.38.100:2453", "http://67.43.236.20:7227", "socks5://166.62.87.148:40591", "socks5://184.170.245.148:4145", "socks5://199.229.254.129:4145", "socks5://184.178.172.17:4145", "socks5://172.93.111.235:43520", "socks5://135.148.10.161:42881", "socks5://107.180.101.226:56316", "socks5://104.238.111.107:26305")
	if err != nil {
		panic(fmt.Errorf("error establishing the round robin proxy server system: %v", err))
	}
	c.SetProxyFunc(rp)

	c.CacheDir = "./scrape_cache"

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("User-Agent", userAgents[userAgentIndex])
		userAgentIndex = (userAgentIndex + 1) % len(userAgents)
		r.Headers.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
		r.Headers.Set("Accept-Language", "en-US,en;q=0.5")
		r.Headers.Set("Accept-Encoding", "gzip, deflate, br")
		r.Headers.Set("Connection", "keep-alive")
		r.Headers.Set("DNT", "1")
		r.Headers.Set("Upgrade-Insecure-Requests", "1")

		// "referer" := strings.Replace(r.URL.String(), "PrinterFriendly=true&", "", 1)
		r.Headers.Set("Referer", "https://www.cellartracker.com/")
	})

	// Set rate limiting and request throttling
	err = c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 1,
		RandomDelay: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("Error setting rate limiting:", err)
	}

	// Enable session management
	extensions.Referer(c)

	SetUserAgent_plugin(c)
	HTMLresponse_plugin(c)
	//	RequestDump_plugin(c)
	//	ResponseDump_plugin(c)

	// define db structure
	db := client.Database("wineDB")
	wineCollection := db.Collection("WINES")

	testingWineCollection(wineCollection)

	// OnHTML callback for rows in the table with class 'editList'... GETS THE TITLE AND POSSIBLE YEAR
	c.OnHTML("table.editList tr.properties", func(e *colly.HTMLElement) {

		// Try to extract the info from the first table of data
		field := e.ChildText("td:nth-child(1) b")
		datum := e.ChildText("td:nth-child(2) a")
		// this will give us most of the basic information we need (vineyard, varietal, appellation, ect. )
		if datum != "" && datum != "n/a" && field != "" {
			hadIssues, field := scrapeTable(e)
			if hadIssues {
				fmt.Printf("Unsuccesful attempt to scrape table for: %v", e.Text)
			} else {
				fmt.Printf("successfully scraped the table for the %v field ", field)
			}

			// so if datum is empty or n/a or field is empty, BUT we have still triggerd this callback, we want to 
			// check if its because we have selected the community tasting notes
		} else if strings.Contains(e.Text, "Community Tasting Notes") {
			wineMapVal := e.Request.Ctx.Get("wineMapVal")
			b := []byte(wineMapVal)
			var wineEntry WineInfo
			err := json.Unmarshal(b, &wineEntry)
			if err != nil {
				fmt.Println("Error marshalling wineEntry:", err)
				return
			}

			// this helper function extracts the ratings from the text!
			myRating, err := extractRatings(e.Text)
			if err != nil {
				fmt.Printf("error extracting ratings: %v\n", err)
				return
			}

			wineEntry.Rating = myRating
			wEntry_serial, err := json.Marshal(wineEntry)
			if err != nil {
				fmt.Println("Error marshalling wineEntry:", err)
				return
			}
			val := string(wEntry_serial)

			e.Request.Ctx.Put("wineMapVal", val)
		}
	})

	// a few tables I found were weird and transposed from usual orientation. So I'vew included this backup logic, which calls the same save fgunction as the table scraping
	c.OnHTML("td a[href]", func(e *colly.HTMLElement) {
		hadIssues, field := scrapeTable(e)
		if hadIssues {
			fmt.Printf("Unsuccesful attempt to scrape table for: %v", e.Text)
		} else {
			fmt.Printf("successfully scraped the table for the %v field ", field)
		}
	})

	// get image URLhadIssues := scrapingTable(e, key, value)
	c.OnHTML("img", func(e *colly.HTMLElement) {
		wineMapVal := e.Request.Ctx.Get("wineMapVal")
		b := []byte(wineMapVal)
		var wInfo WineInfo
		err := json.Unmarshal(b, &wInfo)
		if err != nil {
			fmt.Println("Error unmarshalling wineMapVal:", err)
			return
		}

		allImgSrc := []string{}
		allImgSrc = append(allImgSrc, e.Attr("src"))
		wInfo.ImgSource = allImgSrc

		wineMapVal_serial, err := json.Marshal(wInfo)
		if err != nil {
			fmt.Println("Error marshalling wInfo:", err)
			return
		}
		val := string(wineMapVal_serial)

		e.Request.Ctx.Put("wineMapVal", val)
	})

	c.OnError(func(r *colly.Response, err error) {
		fmt.Println("Request URL:", r.Request.URL, "failed with response:", r, "\nError:", err)
		time.Sleep(1 * time.Second)
	})

	// NOTE, the order of the callbacks is important, it will always go: 1. request, 2. error, 3. response, 4. html, 5. scraped
	// Store the contextr in the request funciton. store the iterator value in there too.
	c.OnRequest(func(r *colly.Request) {
		parsedURL, err := url.Parse(r.URL.String())
		if err != nil {
			fmt.Println("Could not parse URL:", r.URL.String())
			return
		}
		// extract the ITERATION NUMBER from the URL. Assumption is that the URL format is: "...?iWine={itr}&Label={imgNum}"
		query := parsedURL.Query()
		itr := query.Get("iWine")
		fmt.Println("itr:", itr)

		// Store the extracted values in the request context
		r.Ctx.Put("itr", itr)

		wineInfoMap := WineInfo{
			GeoIdentifiers: []string{},
			URL:            r.URL.String(),
			ID:             itr}
		serialized, err := json.Marshal(wineInfoMap)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		// Convert the serialized data to a string
		serializedString := string(serialized)
		r.Ctx.Put("wineMapVal", serializedString)
	})

	//3 this callback checks if trhe url in the response is the same as the one in the request, because if not it measn we got a middfirecrt
	c.OnResponse(func(r *colly.Response) {
		// a clean hole leads to a clean mind!
		finalURL, err := url.Parse(r.Request.URL.String())
		if err != nil {
			fmt.Println("Error parsing URL:", r.Request.URL.String())
			return
		}
		itr := r.Request.Ctx.Get("itr")
		query := finalURL.Query()
		finalURL_itr := query.Get("iWine")
		if finalURL_itr != itr {
			invalidVals = append(invalidVals, string2int_noerror(itr))
			fmt.Printf("Error: itr: %v redirects to value: %v\n", itr, finalURL_itr)
			r.Request.Abort()
			return
		}

		// ensure that gzip is being properly handledq
		contentType := r.Headers.Get("Content-Type")
		fmt.Println("Content-Type:", contentType)
	})

	// this callback evaluates how the scraping went, and catregorizes the results as invalid must
	c.OnScraped(func(r *colly.Response) {
		mapVal_serial := r.Request.Ctx.Get("wineMapVal")
		var WineInfo WineInfo
		err := json.Unmarshal([]byte(mapVal_serial), &WineInfo)
		if err != nil {
			fmt.Println("Error unmarshalling wineMapVal:", err)
			return
		}
		itr := r.Request.Ctx.Get("itr")

		if WineInfo.Title == "" {
			fmt.Println("Error: no title found for itr:", itr)
			toRescrape = append(toRescrape, string2int_noerror(itr))
			WineInfo.Title = "must repeat"

		} else if strings.Contains(WineInfo.Title, "Invalid wine") {
			invalidVals = append(invalidVals, string2int_noerror(itr))
			WineInfo.Title = "Invalid"
		} else {
			scrapedVals = append(scrapedVals, string2int_noerror(itr))
		}

		mapMutex.Lock()
		wineMap[itr] = WineInfo
		mapMutex.Unlock()
	})

	// end of callbacks

	// START SCRAPING
	//maxNumUrls := 100000
	batchSize := 5
	//nBatches := int(math.Ceil(float64(maxNumUrls)/float64(batchSize)) + 1)

	// Loop over batches
	for batchNum := 1; batchNum < 3; batchNum++ {
		myUrls := []string{}
		for i := 1; i <= batchSize; i++ {
			itr := (batchNum-1)*batchSize + i
			imageNumber := 1
			myUrls = append(myUrls, fmt.Sprintf(urlBase, itr, imageNumber))
		}

		// Visit URLs and scrape data
		var wg sync.WaitGroup
		for _, url := range myUrls {
			wg.Add(1)
			go func(u string) {
				defer wg.Done()

				c.Visit(url)

			}(url)
		}
		wg.Wait()

		// Process scraped data and insert into MongoDB collection
		mapMutex.Lock()
		for key, value := range wineMap {
			if wineMap[key].Title == "must repeat" || wineMap[key].Title == "Invalid" {
				continue
			}
			_, err := wineCollection.InsertOne(context.Background(), bson.M{"_id": key, "data": value})
			if err != nil {
				fmt.Println("Error inserting document into MongoDB:", err)
				continue
			}
			fmt.Println("Inserted document into MongoDB:", key)
		}
		wineMap = make(map[string]WineInfo) // Clear wineMap
		mapMutex.Unlock()
	}
}


func scrapeTable(e *colly.HTMLElement) (bool, string) {
	
	// first let us confirm that there really is data here
	elem := e.Attr("href")

	// Split the href by '='
	parts := strings.Split(elem, "=")
	
	// If there are at least 2 parts, return
	if len(parts) <= 1 {return true, ""}

	// this wqill eventually go into a map as a key value pair. The key will be the field and the datum will be the value
	field := parts[len(parts)-2]
	datum := parts[len(parts)-1]

	// pull the wineMapVal from the context (it would be a mistake to pull this value before checking the string for sufficient length and structure)
	wineMapVal := e.Request.Ctx.Get("wineMapVal")
	b := []byte(wineMapVal)
	var wineEntry WineInfo
	err := json.Unmarshal(b, &wineEntry)
	if err != nil {
		fmt.Println("Error unmarshalling wineMapVal:", err)
		return true, ""
	}

	switch field {
	case "Producer": // often this field has a little URL link that takes you off the cellartracker wbesite and brings you to the page of said producers 
		if strings.Contains(datum, "(web)") {
			datum = strings.Split(datum, "(web)")[0]
		}
		wineEntry.Producer = datum
	case "Vineyard":
		wineEntry.Vineyard = datum
	case "Variety":
		wineEntry.GrapeVarietal = datum
	case "Type":
		wineEntry.Type = datum
	case "Designation":
		wineEntry.Designation = datum
	case "Appellation":
		wineEntry.Appellation = datum
		wineEntry.GeoIdentifiers = append(wineEntry.GeoIdentifiers, datum)
	case "Country":
		wineEntry.Country = datum
		wineEntry.GeoIdentifiers = append(wineEntry.GeoIdentifiers, datum)
	case "Region":
		wineEntry.GeoIdentifiers = append(wineEntry.GeoIdentifiers, datum)
	case "Subregion":
		wineEntry.GeoIdentifiers = append(wineEntry.GeoIdentifiers, datum)
	}
	wEntry_serial, err := json.Marshal(wineEntry)

	if err != nil {
		fmt.Println("Error marshalling wineEntry:", err)
		return true,""
	}

	val := string(wEntry_serial)
	e.Request.Ctx.Put("wineMapVal", val)
	return false, field
}


// HELPER FUNCTIONS

// SetUserAgent is a plugin to set a custom User-Agent for all requests.each timne cyucling throuhgh a new now one of the group
func SetUserAgent_plugin(c *colly.Collector) {
	uaIndex := int(time.Now().Unix()) % len(userAgents)
	userAgent := userAgents[uaIndex]
	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("User-Agent", userAgent)
	})
}

func HTMLresponse_plugin(c *colly.Collector) {
	c.OnRequest(func(r *colly.Request) {
		url := r.URL.String()

		// Perform a preliminary GET request
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println("Error making preliminary request:", err)
			return
		}
		defer resp.Body.Close()

		// Check the response status code
		description := categorizeStatusCode(resp.StatusCode)
		if description == "redirected" {
			// Status code indicates a redirect
			fmt.Printf("URL resp has status code: %v; is likely to be redirected:%v... aborting\n ", resp.StatusCode, url)
			invalidVals = append(invalidVals, string2int_noerror(r.Ctx.Get("itr")))
			r.Abort()
			return
		}
		fmt.Printf("URL resp has status code: %v; unlikely that it be redirected:%v\n", resp.StatusCode, url)

		// Request seems valid, allow it to proceed
		fmt.Println("Allowing request to proceed:", url)
	})
}

func extractRatings(ratingsText string) (RatingInfo, error) {
	myRating := RatingInfo{
		AverageRating: 0,
		MedianRating:  0,
		NumRatings:    0,
	}
	averageRegex := regexp.MustCompile(`average (\d+(?:\.\d+)?)`)
	averageMatches := averageRegex.FindStringSubmatch(ratingsText)

	if len(averageMatches) > 1 {
		avgMatchFloat, err := strconv.ParseFloat(averageMatches[1], 64)
		if err != nil {
			return myRating, fmt.Errorf("error converting average rating (=%v) to float64: %v", averageMatches[1], err)
		}
		myRating.AverageRating = avgMatchFloat
	}

	medianRegex := regexp.MustCompile(`median of (\d+)`)
	medianMatches := medianRegex.FindStringSubmatch(ratingsText)
	if len(medianMatches) > 1 {
		medianRatingFloat, err := strconv.ParseFloat(medianMatches[1], 64)

		if err != nil {
			return myRating, fmt.Errorf("error converting median rating of %v to float:%v", medianMatches[1], err)
		}
		myRating.MedianRating = medianRatingFloat
	}

	countRegex := regexp.MustCompile(`in (\d+) notes`)
	countMatches := countRegex.FindStringSubmatch(ratingsText)
	if len(countMatches) > 1 {
		myRating.NumRatings = string2int_noerror(countMatches[1])
	}

	return myRating, nil
}

func string2int_noerror(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Errorf("failed to convert '%s' to int: %v", s, err))
	}
	return i
}

func testingWineCollection(wineCollection *mongo.Collection) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	test := WineInfo{Title: "test"}

	// Inserting test data
	result1, err := wineCollection.InsertOne(ctx, test)
	if err != nil {
		fmt.Println("Error inserting test data into db:", err)
		panic(err)
	}
	fmt.Printf("Inserted test data into db with id: %v\n", result1.InsertedID)

	time.Sleep(100 * time.Millisecond)

	// Counting documents
	opts := options.Count().SetMaxTime(10 * time.Second)
	count, err := wineCollection.CountDocuments(ctx, bson.M{"title": "test"}, opts)
	if err != nil {
		panic(fmt.Errorf("error counting test documents in collection: %v", err))
	}

	if count == 0 {
		fmt.Println("Error: no test documents found in collection... ")

	} else {
		fmt.Printf("found %v test documents found in collection... deleting now", count)

		// Deleting test data
		deleteOpts := options.Delete().SetCollation(&options.Collation{
			Locale:    "en_US",
			Strength:  1,
			CaseLevel: false,
		})
		var result3 *mongo.DeleteResult

		if count == 1 {
			result3, err = wineCollection.DeleteOne(ctx, bson.M{"title": "test"}, deleteOpts)
		} else {
			result3, err = wineCollection.DeleteMany(ctx, bson.M{"title": "test"}, deleteOpts)
		}
		if err != nil {
			panic(fmt.Errorf("error deleting test data from db: %v", err))
		}

		fmt.Printf("Successfully deleted %v test documents!\n", result3.DeletedCount)

	}
	fmt.Println("Testing of wineCollection operations: successful!")
}

func RequestDump_plugin(c *colly.Collector) {

	c.OnRequest(func(r *colly.Request) {
		// Create a new http.Request
		httpReq, err := http.NewRequest(r.Method, r.URL.String(), nil)
		if err != nil {
			fmt.Println("Error creating http.Request:", err)
			return
		}

		// Copy headers from colly.Request to http.Request
		for key, values := range *r.Headers {
			for _, value := range values {
				httpReq.Header.Add(key, value)
			}
		}

		// Set the request body if available
		if r.Body != nil {
			// Convert io.Reader to io.ReadCloser
			httpReq.Body = io.NopCloser(r.Body)
		}

		// Dump the http.Request
		dump, err := httputil.DumpRequestOut(httpReq, false)
		if err != nil {
			fmt.Println("Error dumping request:", err)
			return
		}

		fmt.Println("Request Dump:")
		fmt.Println(string(dump))
	})
}

func ResponseDump_plugin(c *colly.Collector) {
	c.OnResponse(func(r *colly.Response) {
		// Create an http.Response object
		httpResp := &http.Response{
			StatusCode: r.StatusCode,
			Header:     *r.Headers,
			Body:       io.NopCloser(bytes.NewReader(r.Body)),
		}

		// Dump the http.Response
		dump, err := httputil.DumpResponse(httpResp, false)
		if err != nil {
			fmt.Println("Error dumping response:", err)
			return
		}

		fmt.Println("Response Dump:")
		fmt.Println(string(dump))
	})
}

func categorizeStatusCode(statusCode int) string {

	if statusCode > 199 && statusCode < 300 {
		return "good"
	}
	if statusCode > 299 && statusCode < 400 {
		return "redirected"
	}
	if statusCode > 399 && statusCode < 600 {
		return "error"
	}
	return "unknown"
}
