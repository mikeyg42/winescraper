const fs = require('fs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const userAgents = require('./userAgents.json').userAgents;
const headers = require('./headers.json').headers;

puppeteer.use(StealthPlugin({        
    delayMs: 2000, // Base delay in milliseconds
    delayDeviation: 800, // Deviation from the base delay in milliseconds
    showBrowser: false, // If true, the browser window will be displayed;
}));

async function scrapeWebsite(myHeader, url, id) {
  console.log(`Scraping ${url} with user agent ${myHeader['User-Agent']}`);
  const startTime = Date.now();

  try {
    const browser = await puppeteer.launch({
      headless: true,
    });
    const page = await browser.newPage();
    await page.setExtraHTTPHeaders(myHeader);
    await page.setUserAgent(myHeader['User-Agent']);

    const navigationPromise = page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });
    await navigationPromise;
   
    const delay = Math.floor(Math.random() * 4000) + 3000;
    await new Promise((resolve) => setTimeout(resolve, delay));

   console.log(`Page loaded successfully for ${url}`);

    // Extract fields from the DOM
    const data = await page.evaluate(() => {
      const titleElement = document.querySelector('#hproduct > table.banner > tbody > tr > td > h1 > span.fn');
      return titleElement ? titleElement.innerText.trim() : null;
    });

    const rating = await page.evaluate(() => {
      const ratingElement = document.querySelector("#hproduct > table:nth-child(8) > tbody > tr:nth-child(1) > td:nth-child(2) > h3 > i > span.rating > span.average");
      return ratingElement ? ratingElement.innerText.trim() : null;
    });

    //console.log(`Data extracted successfully for ${url}`);

    // Take a full-page screenshot and save it to the external drive
    const screenshotPath = `/Volumes/MOVESPEED/screenshots/${id}_${data || 'untitled'}_fullpage_01.png`;
    await page.screenshot({ path: screenshotPath, fullPage: true });

    const endTime = Date.now();
    const scrapingTime = endTime - startTime;
    console.log(`Scraping completed for ${url} in ${scrapingTime}ms`);

    await browser.close();
 //   console.log(`Browser closed successfully for ${url}`);

    return { success: true, data: { id, title: data, rating }, scrapingTime };
  } catch (error) {
    console.error(`Error scraping ${url}:`, error);
    return { success: false, error };
  }
}

async function main() {
  const failedUserAgents = new Set();
  const scrapedData = [];
  const scrapingTimes = [];

  for (let i = 1000; i <= 1500; i++) {
    const url = `https://www.cellartracker.com/classic/wine.asp?PrinterFriendly=true&iWine=${i}`;
    let userAgentIndex = i % userAgents.length;

    while (true) {
      const myUserAgent = userAgents[userAgentIndex];
      const myHeader = { ...headers, 'User-Agent': myUserAgent };

      if (failedUserAgents.has(myUserAgent)) {
        userAgentIndex = (userAgentIndex + 1) % userAgents.length;
        continue;
      }

      const { success, data, error, scrapingTime } = await scrapeWebsite(myHeader, url, i);

      if (success) {
        // Process the scraped data
        console.log(`Successfully scraped ${url}`);
        scrapedData.push(data);
        scrapingTimes.push(scrapingTime);
        break;ƒ
      } else {
        if (error.message.includes('403') || error.message.includes('404')) {
          console.log(`User agent ${myUserAgent} failed for ${url}. Removing it from the list.`);
          failedUserAgents.add(myUserAgent);
        } else {
          console.log(`Skipping ${url} due to an anomaly:`, error);
          break;
        }
      }
    }

    // Wait for a random amount of time to reduce server pressure and mimic human behavior
    const waitTime = Math.random() * 9000 + 3000; // Random wait time between 3 and 12 seconds
   // console.log(`Waiting for ${waitTime}ms before proceeding to the next iteration`);
    await new Promise((resolve) => setTimeout(resolve, waitTime));
  }

  // Save the scraped data to a JSON file
  fs.writeFileSync('scrapedData.json', JSON.stringify(scrapedData, null, 2));
  console.log('Scraped data saved to scrapedData.json');

  // Calculate benchmarking statistics
  const totalScrapingTime = scrapingTimes.reduce((sum, time) => sum + time, 0);
  const averageScrapingTime = totalScrapingTime / scrapingTimes.length;
  const medianScrapingTime = getMedian(scrapingTimes);
  const maxScrapingTime = Math.max(...scrapingTimes);
  const minScrapingTime = Math.min(...scrapingTimes);
  const stdDevScrapingTime = getStandardDeviation(scrapingTimes);

  console.log('Benchmarking Statistics:');
  console.log(`Total Scraping Time: ${totalScrapingTime}ms`);
  console.log(`Average Scraping Time: ${averageScrapingTime}ms`);
  console.log(`Median Scraping Time: ${medianScrapingTime}ms`);
  console.log(`Max Scraping Time: ${maxScrapingTime}ms`);
  console.log(`Min Scraping Time: ${minScrapingTime}ms`);
  console.log(`Standard Deviation of Scraping Time: ${stdDevScrapingTime}ms`);


}


function getMedian(arr) {
    const sorted = arr.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  }
  
  function getStandardDeviation(arr) {
    const n = arr.length;
    const mean = arr.reduce((sum, val) => sum + val, 0) / n;
    const variance = arr.reduce((sum, val) => sum + (val - mean) ** 2, 0) / (n - 1);
    return Math.sqrt(variance);
  }
  
  main().catch((error) => console.error('An error occurred:', error));