package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly"
	"github.com/gocolly/colly/extensions"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type wine struct {
	URL            string   `bson:"url"`
	Title          string   `bson:"title"`
	Vineyard       string   `bson:"vineyard"`
	Year           int      `bson:"year"`
	Rating         int      `bson:"rating"`
	Country        string   `bson:"country"`
	geoIdentifiers []string `bson:"geoidentifiers"`
	GrapeVarietal  []string `bson:"grape_varietal"`
	TypeTags       []string `bson:"type_tags"`
	PriceRange     string   `bson:"price_range"`
	FlavorTags     []string `bson:"flavor_tags"`
	ImgScraped     bool     `bson:"img_scraped"`
}

var urlBase = "https://www.cellartracker.com/classic/wine.asp?iWine=%d&Label=%d"

var wineProdCountries = []string{"Italy", "Spain", "France", "United States", "China", "Argentina", "Chile", "Australia", "South Africa", "Germany", "Portugal", "Romania", "Greece", "Russia", "New Zealand", "Brazil", "Hungary", "Austria", "Serbia", "Moldova", "Bulgaria", "Georgia", "Switzerland", "Ukraine", "Japan", "Peru", "Uruguay", "Canada", "Algeria", "Czech Republic", "North Macedonia", "Croatia", "Turkey", "Mexico", "Turkmenistan", "Morocco", "Uzbekistan", "Slovakia", "Belarus", "Albania", "Kazakhstan", "Tunisia", "Montenegro", "Lebanon", "Slovenia", "Colombia", "Luxembourg", "Cuba", "Estonia", "Cyprus", "Azerbaijan", "Bolivia", "Madagascar", "Bosnia and Herzegovina", "Armenia", "Lithuania", "Egypt", "Israel", "Belgium", "Latvia", "Malta", "Zimbabwe", "Kyrgyzstan", "Paraguay", "Ethiopia", "Jordan", "United Kingdom", "Costa Rica", "Panama", "Tajikistan", "Liechtenstein", "Syria", "Poland", "Réunion"}

var (
	wineMap  = make(map[int]wine)
	mapMutex = &sync.Mutex{}

	emptyTitleCounter int
	stopScraping      = make(chan bool, 1) // Channel to signal stopping
)

func main() {

	// CONNECT TO ATLAS DATABASE https://cloud.mongodb.com/v2/65da774c19ccda13b572907b#/clusters

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// have URI saved as an environment variable
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(os.Getenv("ATLAS_URI")).SetServerAPIOptions(serverAPI)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	// JUST TO BE SURE OUR CLUSTER IS REALLY CONNECTED... Send a ping to confirm a successful connection
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Err(); err != nil {
		panic(err)
	}
	fmt.Println("Pinged your deployment. You successfully connected to MongoDB!")

	// CONNECTION TO DATABASE SUCCESSFUL

	// START INITIALIZING COLLY

	c := colly.NewCollector(
		colly.MaxDepth(0), // meaning the links on the main visited page and the linkes on THOSE pages

		colly.Async(true),

		// Cache responses to prevent multiple download of pages , even if the collector is restarted
		colly.CacheDir("./cache"),
	)

	//Colly uses HTTP keep-alive to enhance scraping speed which requires open file descriptor...
	// ... and so nmax-fd limit can be easily reached with long running jobs.
	c.WithTransport(&http.Transport{
		DisableKeepAlives: true,
	})

	// Set up concurrency limit
	wine_limitRule := &colly.LimitRule{
		DomainGlob:  urlBase,         // Adjust based on the actual domain
		Parallelism: 5,               // Number of parallel requests
		RandomDelay: 2 * time.Second, // Delay between requests to the same domain
	}
	c.Limit(wine_limitRule)

	extensions.RandomUserAgent(c)

	// make a second collector that will just be for images!!
	cImg := c.Clone()

	// add datumn to DB to set headings and db structure
	db := client.Database("wineDB")
	wineCollection := db.Collection("WINES")
	img_db := client.Database("imageDB")
	bucket, _ := gridfs.NewBucket(
		img_db,
	)

	var wineEntry []wine
	wineCollection.InsertOne(ctx, wineEntry)

	// Extract title and parse info from it
	c.OnHTML(`meta[property="og:title"]`, func(e *colly.HTMLElement) {
		newEntry := wine{}
		itr, err := strconv.Atoi(e.Request.Ctx.Get("itr"))
		if err != nil {
			log.Println("Error converting itr to int:", err)
			return
		}

		newEntry.URL = fmt.Sprintf(urlBase, itr, 1)

		title := e.Attr("content")
		fmt.Println("Title:", title)
		newEntry.Title = title

		// Extract year
		re := regexp.MustCompile(`(19\d{2}|20[0-4]\d|2050)`)
		yearMatches := re.FindStringSubmatch(title)
		var wineYear int

		if len(yearMatches) > 1 {
			fmt.Printf("Multiple years found in %s\n", title)
			wineYear, err = strconv.Atoi(yearMatches[0])
			if err != nil {
				log.Println("Error converting year to int:", err)
				return
			}
		} else if len(yearMatches) == 1 {
			wineYear, err = strconv.Atoi(yearMatches[0])
			if err != nil {
				log.Println("Error converting year to int:", err)
				return
			}
		} else if len(yearMatches) == 0 {
			wineYear = 0
			fmt.Printf("no year found in %s\n", title)
		}
		newEntry.Year = wineYear

		countryFound := 0
		for _, country := range wineProdCountries {
			if strings.Contains(title, country) {
				countryFound++
				if countryFound >= 2 {
					fmt.Println("CountryError:", fmt.Sprintf("Multiple countries found in title: originally had %s, now have %s\n", newEntry.Country, country))
					break
				}
				parts := strings.Split(title, country)
				vineyard := parts[0]
				geography := append([]string{country}, parts[1])

				newEntry.Vineyard = vineyard
				newEntry.Country = country
				newEntry.geoIdentifiers = geography

			}
		}

		mapMutex.Lock()
		wineEntry, exists := wineMap[itr]

		if exists {
			wineEntry.Year = newEntry.Year
			wineEntry.Title = newEntry.Title
			wineEntry.URL = newEntry.URL
			wineEntry.Vineyard = newEntry.Vineyard
			wineEntry.Country = newEntry.Country
			wineEntry.geoIdentifiers = newEntry.geoIdentifiers
		} else {
			wineMap[itr] = newEntry
		}
		mapMutex.Unlock()
	})

	cImg.OnHTML("img", func(e *colly.HTMLElement) {
		imgSrc := e.Attr("src")

		// Download image
		resp, err := http.Get(imgSrc)
		if err != nil {
			log.Println("Error downloading image:", err)
			return
		}
		defer resp.Body.Close()

		imgData, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Println("Error reading image data:", err)
			return
		}

		// fetch the iterator #, which will be saved as a string
		itr := e.Request.Ctx.Get("itr")

		// Store in GridFS
		uploadStream, err := bucket.OpenUploadStream("img" + itr + ".jpg")
		if err != nil {
			log.Fatal(err)
		}
		defer uploadStream.Close()

		_, err = uploadStream.Write(imgData)
		if err != nil {
			log.Fatal(err)
		}

		itr_int, err := strconv.Atoi(itr)
		if err != nil {
			log.Println("Error converting itr to int:", err)
			return
		}

		mapMutex.Lock()
		wineEntry, exists := wineMap[itr_int]
		if exists {
			wineEntry.ImgScraped = true
		} else {
			wineMap[itr_int] = wine{ImgScraped: true}
		}
		mapMutex.Unlock()

		log.Println("Image stored in GridFS")
	})

	// NOTE, the order of the callbacks is important, it will always go: 1. request, 2. error, 3. response, 4. html, 5. scraped
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL)
	})

	c.OnError(func(_ *colly.Response, err error) {
		log.Println("Something went wrong: ", err)
	})

	c.OnResponse(func(r *colly.Response) {
		fmt.Println("Page visited: ", r.Request.URL)
	})

	c.OnScraped(func(r *colly.Response) {
		// Extract the iterator number from the context
		itrStr := r.Ctx.Get("itr")
		itr_int, err := strconv.Atoi(itrStr)
		if err != nil {
			log.Println("Error converting itr to int:", err)
			return
		}

		mapMutex.Lock()
		entry, exists := wineMap[itr_int]

		if !exists || entry.Title == "" {
			emptyTitleCounter++
		} else {
			emptyTitleCounter = 0
			opts := options.InsertOne()
			opts = opts.SetComment(fmt.Sprintf("Inserting new wine data into db: %v\n", itrStr))
			_, err = wineCollection.Database().Collection("WINES").InsertOne(context.Background(), entry, opts)
			if err != nil {
				fmt.Printf("Error inserting wine #%v into database: %v\n", itrStr, err)
			}
		}

		mapMutex.Unlock()

		if emptyTitleCounter >= 20 {
			fmt.Println("20 consecutive pages with no titles found, stopping.")
			stopScraping <- true // Send signal to stop scraping
		}

	})

	go func() {
		for itr := 1; itr <= 600; itr++ {

			select {
			case <-stopScraping:
				fmt.Println("Stopping signal received, ending scraping.")
				return
			default:
				// Check if this itr's data already exists in MongoDB
				var result wine
				filter := bson.M{"url": fmt.Sprintf(urlBase, itr, 1)}
				err := wineCollection.FindOne(ctx, filter).Decode(&result)
				if err == mongo.ErrNoDocuments {
					// Document not found, all good to scrape
					url := fmt.Sprintf(urlBase, itr, 1)
					ctx := colly.NewContext()
					ctx.Put("itr", strconv.Itoa(itr))
					ctx.Put("opts", opts)

					c.Request("GET", url, nil, ctx, nil)
					cImg.Request("GET", url, nil, ctx, nil)
				} else if err != nil {
					fmt.Printf("Error checking for existing document (itr=%d): %v\n", itr, err)

				} else {
					// Document exists, skip scraping for this itr
					fmt.Printf("Skipping itr=%d, already exists.\n", itr)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Start scraping
	c.Wait()
	cImg.Wait()
}
