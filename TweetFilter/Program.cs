using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Diagnostics;

//Comman line solution to the IQVIA bad api.
//The user can input a start and end date and the application will report the distinct tweets within the 
//given time period

namespace AllTweets
{
    //Tweet Model
    public class Tweet
    {
        public string id { get; set; }
        public DateTime stamp { get; set; }
        public string text { get; set; }
    }

    //Helper class to check for duplicate tweets
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
        // Use HttpClient.
        static HttpClient client;
        static Newtonsoft.Json.JsonSerializerSettings js;

        //Initialize comparer to check for duplcate twees
        static TweetComparer tweetComp = new TweetComparer();

        //List of tweets with no duplicates
        static List<Tweet> filteredtweets;

        static void Main(string[] args)
        {
            //Set window to be wider than default
            Console.WindowWidth = (int)(0.8 * Console.LargestWindowWidth);

            //Increase the buffer height to show all tweets
            Console.BufferHeight = 20000;

            //Get dates from user and check if they are valid
            if (!GetDatesFromUser(out string startDate, out string endDate))
                return;

            Console.WriteLine("Downloading tweets...");

            //Retrieve and output tweets
            DownloadTweets(startDate, endDate);

            // ... Display the results
            foreach (var tweet in filteredtweets)
            {
                Console.WriteLine("ID: {0}, {1}", tweet.id, tweet.stamp);
                Console.WriteLine("Tweet: {0}\n", tweet.text);
            }

            Console.WriteLine("Total number of distinct tweets between {0} and {1} is {2}", startDate, endDate, filteredtweets.Count);
            Console.ReadLine();
        }

        private static void DownloadTweets(string startDate, string endDate)
        {
            //Stopwatch sw = new Stopwatch();
            //sw.Start();

            filteredtweets = new List<Tweet>();

            // Target.
            string strGetRequest = "api/v1/Tweets?startDate=" + startDate + "&endDate=" + endDate;

            // Use HttpClient.
            client = new HttpClient()
            {
                BaseAddress = new Uri(baseURI)
            };

            try
            {
                var tweetResult = GetTweets(strGetRequest).Result;
                if (tweetResult == null)
                    return;

                //Populate filtered list with the first Get results
                filteredtweets = tweetResult.ToList();

                // Now loop until we have reached the desired end date. 
                while (tweetResult.Count > 1)
                {
                    //Set new start date to the date of the last item in the previous set of tweets
                    var newStartDate = tweetResult[tweetResult.Count - 1].stamp.ToString("yyyy-MM-ddTHH:mm:ssZ");
                    strGetRequest = "api/v1/Tweets?startDate=" + newStartDate + "&endDate=" + endDate;

                    tweetResult = GetTweets(strGetRequest).Result;
                    if (tweetResult == null)
                        return;

                    //Get number of tweets
                    int nCount = tweetResult.Count;

                    //Avoid duplicates in the get
                    tweetResult = tweetResult.Distinct<Tweet>(tweetComp).AsParallel().ToList<Tweet>();

                    //Add to filtered tweets
                    filteredtweets.AddRange(tweetResult);

                    //If less than 100 we are getting the last bit of tweets for the desired date range
                    if (nCount < 100)
                        break;

                }

            }
            catch (Exception e)
            {
                string error = e.ToString();
                Console.WriteLine(error);
                return;
            }

            //Ensure last add tweets are distinct
            filteredtweets = filteredtweets.Distinct<Tweet>(new TweetComparer()).AsParallel().ToList<Tweet>();

            // sw.Stop();
            // var et = sw.ElapsedMilliseconds.ToString();


        }

        private static async Task<List<Tweet>> GetTweets(string strGetRequest)
        {
            try
            {
                //Make the get request
                HttpResponseMessage response = await client.GetAsync(strGetRequest);

                var responsePhrase = response.ReasonPhrase;
                if (!responsePhrase.Equals("OK"))
                {
                    Console.WriteLine("Get request failed due to {0}", responsePhrase);
                    return null;
                }

                HttpContent content = response.Content;

                // ... Read the string.
                string result = await content.ReadAsStringAsync();

                //Initialize deserialization with datetime using UTC
                js = new Newtonsoft.Json.JsonSerializerSettings()
                {
                    DateTimeZoneHandling = Newtonsoft.Json.DateTimeZoneHandling.Utc
                };

                //Get the list of responses
                var tweetResult = Newtonsoft.Json.JsonConvert.DeserializeObject<List<Tweet>>(result, js);

                tweetResult.Distinct<Tweet>(tweetComp).AsParallel().ToList<Tweet>();

                //Populate filtered list with the first Get results
                return tweetResult;

            }
            catch (Exception e)
            {
                string error = e.ToString();
                Console.WriteLine(error);
            }

            return null;

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
