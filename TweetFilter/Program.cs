using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace TweetFilter
{
    //Tweet Model
    public class Tweet
    {
        public string id { get; set; }
        public DateTime stamp { get; set; }
        public string text { get; set; }
    }

    //Class to check for duplicate tweets
    public class TweetComparer : IEqualityComparer<Tweet>
    {
        public bool Equals(Tweet x, Tweet y)
        {
            if (x == null || y == null)
                return false;
            else if (x.text == y.text)
                return true;
            else
                return false;
        }

        public int GetHashCode(Tweet obj)
        {
            return obj.text.GetHashCode();
        }
    }

    class Program
    {
        static string baseURI = "https://badapi.Iqvia.io/";

        static void Main(string[] args)
        {
            //Set window to be a bit wider than default
            Console.WindowWidth = (int)(0.8 * Console.LargestWindowWidth);

            //Increase the buffer height to attempt to show all tweets
            Console.BufferHeight = 20000;

            //Get dates from user and check if they are valid
            if (!GetDatesFromUser(out string startDate, out string endDate))
                return;

            Console.WriteLine("Downloading tweets...");

            //Retrieve and output tweets
            DownloadTweets(startDate, endDate);

            Console.ReadLine();
        }

        static async void DownloadTweets(string startDate, string endDate)
        {
            //List of tweets with no duplicates
            List<Tweet> filteredtweets = new List<Tweet>();

            // Target.
            string strGetRequest = "api/v1/Tweets?startDate=" + startDate + "&endDate=" + endDate;

            // Use HttpClient.
            HttpClient client = new HttpClient()
            {
                BaseAddress = new Uri(baseURI)
            };

            try
            {
                //Make the get request
                HttpResponseMessage response = await client.GetAsync(strGetRequest);

                var responsePhrase = response.ReasonPhrase;
                if (!responsePhrase.Equals("OK"))
                {
                    Console.WriteLine("Get request failed due to {0}", responsePhrase);
                    return;
                }

                HttpContent content = response.Content;

                // ... Read the string.
                string result = await content.ReadAsStringAsync();

                //Initialize deserialization with datetime using UTC
                Newtonsoft.Json.JsonSerializerSettings js = new Newtonsoft.Json.JsonSerializerSettings()
                {
                    DateTimeZoneHandling = Newtonsoft.Json.DateTimeZoneHandling.Utc
                };

                //Get the list of responses
                var tweetResult = Newtonsoft.Json.JsonConvert.DeserializeObject<List<Tweet>>(result, js);

                //Initialize comparer to check for duplcate twees
                var tweetComp = new TweetComparer();
                var tweetResultDistinct = tweetResult.Distinct<Tweet>(tweetComp).AsParallel().ToList<Tweet>();

                //Populate filtered list with the first Get results
                filteredtweets = tweetResultDistinct.ToList();

                // Now loop until we have reached the end date. We break out when we have a retrieval count < 100
                while (tweetResult.Count > 0)
                {
                    //Set new start date to the date of the last item in the previous set of tweets
                    var newStartDate = tweetResultDistinct[tweetResultDistinct.Count - 1].stamp.ToString("yyyy-MM-ddTHH:mm:ssZ");
                    strGetRequest = "api/v1/Tweets?startDate=" + newStartDate + "&endDate=" + endDate;

                    response = await client.GetAsync(strGetRequest);
                    content = response.Content;

                    // ... Read the string.
                    result = await content.ReadAsStringAsync();

                    tweetResult = Newtonsoft.Json.JsonConvert.DeserializeObject<List<Tweet>>(result, js);

                    //Avoid duplicates in the get
                    tweetResultDistinct = tweetResult.Distinct<Tweet>(tweetComp).AsParallel().ToList<Tweet>();

                    //Add to the filtered tweet list
                    foreach (var tweet in tweetResultDistinct)
                    {
                        //Avoid adding duplicates to the full filtered list
                        if (!filteredtweets.Contains(tweet, tweetComp))
                            filteredtweets.Add(tweet);
                    }

                    //We have reached the final group of tweets within the date range
                    if (tweetResult.Count < 100)
                        break;
                }

            }
            catch (Exception e)
            {
                string error = e.ToString();
                Console.WriteLine(error);
                return;
            }


            // ... Display the results
            foreach (var tweet in filteredtweets)
            {
                Console.WriteLine("ID: {0}, {1}", tweet.id, tweet.stamp);
                Console.WriteLine("Tweet: {0}\n", tweet.text);
            }

            Console.WriteLine("Total number of unique tweets between {0} and {1} is {2}", startDate, endDate, filteredtweets.Count);

        }

        static bool AreDatesValid(string start, string end)
        {
            var startDate = new DateTime();
            if (!DateTime.TryParse(start, out startDate))
            {
                Console.WriteLine("Invalid start date");
                return false;
            }

            var endDate = new DateTime();
            if (!DateTime.TryParse(end, out endDate))
            {
                Console.WriteLine("Invalid end date");
                return false;
            }

            if (DateTime.Compare(startDate, endDate) > 0)
            {
                Console.WriteLine("Start date cannot be after the end date.");
                return false;
            }

            if (DateTime.Compare(startDate, endDate) == 0)
            {
                Console.WriteLine("Start date and end date cannot be the same value.");
                return false;
            }

            return true;
        }

        static bool GetDatesFromUser(out string startDate, out string endDate)
        {
            startDate = "";
            endDate = "";

            var DatesValid = false;

            while (!DatesValid)
            {
                Console.WriteLine("Please enter the start date for tweet retrieval (ex. 2016-01-01T00:00:00, note: time is optional). Press q to quit");
                startDate = Console.ReadLine();

                if (startDate.ToLower() == "q")
                    return false;

                Console.WriteLine("Please enter the end date for tweet retrieval (ex. 2018-01-01T00:00:00, note: time is optional). Press q to quit");
                endDate = Console.ReadLine();

                if (endDate.ToLower() == "q")
                    return false;

                DatesValid = AreDatesValid(startDate, endDate);

            }

            return true;
        }
    }
}
