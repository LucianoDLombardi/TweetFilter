using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Diagnostics;

namespace AllTweets
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

    class GetTweetsCL
    {
        static readonly string baseURI = "https://badapi.Iqvia.io/";

        // Use HttpClient.
        static HttpClient client;
        static Newtonsoft.Json.JsonSerializerSettings js;

        //Initialize comparer to check for duplcate twees
        static readonly TweetComparer tweetComp = new TweetComparer();

        static void Main(string[] args)
        {
            //Adjust the console window
            SetupDisplay();

            //User input dates
            var validStartDate = new DateTime(0);
            var validEndDate = new DateTime(0);

            //Get Dates from user or user pressed q to quit
            var userQuit = false;

            while (true)
            {
                (userQuit, validStartDate, validEndDate) = GetDatesFromUser();

                if (userQuit)
                    return;

                Console.Clear();
                Console.WriteLine("Downloading tweets...");

                Parallel_GetAllTweetsInDateRange(validStartDate, validEndDate);
            }


        }

        static void SetupDisplay()
        {
            //Set window to be a bit wider than default
            Console.WindowWidth = (int)(0.8 * Console.LargestWindowWidth);

            //Increase the buffer height to show all tweets
            Console.BufferHeight = 20000;
        }

        static bool InitializeHTTP()
        {
            try
            {
                // Use HttpClient.
                client = new HttpClient()
                {
                    BaseAddress = new Uri(baseURI)
                };

                //Initialize deserialization with datetime using UTC
                js = new Newtonsoft.Json.JsonSerializerSettings()
                {
                    DateTimeZoneHandling = Newtonsoft.Json.DateTimeZoneHandling.Utc
                };
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to initilize HTTP: " + e.ToString());
                return false;
            }

            return true;
        }

        static public void Parallel_GetAllTweetsInDateRange(DateTime startDate, DateTime endDate)
        {
            if (!InitializeHTTP())
                return;

            //Number of partitions for parrallel download
            int nPartitions = Environment.ProcessorCount;

            //List of Lists for parallel download
            List<List<Tweet>> allTweets = new List<List<Tweet>>();
            for (int n = 0; n < nPartitions; n++)
                allTweets.Add(new List<Tweet>());

            // Stopwatch sw = new Stopwatch();
            // sw.Start();

            //Get the user defined date range in ticks
            var dateRangeTicks = endDate.Ticks - startDate.Ticks;
            var dateRangeIntervalTicks = (long)Math.Ceiling((double)dateRangeTicks / nPartitions);

            //Retrieve  tweets
            Parallel.For(0, allTweets.Count, n =>
            {
                long startTicks = startDate.Ticks + (long)n * dateRangeIntervalTicks;
                long endTicks = startDate.Ticks + (long)(n + 1) * dateRangeIntervalTicks;
                string start = new DateTime(startTicks).ToString();
                string end = new DateTime(endTicks).ToString();

                allTweets[n] = DownloadTweets(start, end);
            });

            // sw.Stop();
            // var et = sw.ElapsedMilliseconds.ToString();

            List<Tweet> allTweetsCombined = new List<Tweet>();
            for (int n = 0; n < nPartitions; n++)
            {
                allTweetsCombined.AddRange(allTweets[n]);
            }

            //Remove final duplicates
            allTweetsCombined = allTweetsCombined.Distinct<Tweet>(new TweetComparer()).AsParallel().ToList<Tweet>();

            // ... Display the results
            foreach (var tweet in allTweetsCombined)
            {
                Console.WriteLine("ID: {0}, {1}", tweet.id, tweet.stamp);
                Console.WriteLine("Tweet: {0}\n", tweet.text);
            }

            Console.WriteLine("Total number of distinct tweets between {0} and {1} is {2}\n", startDate, endDate, allTweetsCombined.Count);
        }

        private static List<Tweet> DownloadTweets(string startDate, string endDate)
        {
            //List of tweets with no duplicates
            List<Tweet> filteredtweets = new List<Tweet>();

            //Stopwatch sw = new Stopwatch();
            //sw.Start();

            // Target.
            string strGetRequest = "api/v1/Tweets?startDate=" + startDate + "&endDate=" + endDate;

            try
            {
                var tweetResult = GetTweetsAsync(strGetRequest).Result;
                if (tweetResult == null)
                    return null;

                //Populate filtered list with the first Get results
                filteredtweets = tweetResult.ToList();

                // Now loop until we have reached the desired end date. 
                while (tweetResult.Count > 1)
                {
                    //Set new start date to the date of the last item in the previous set of tweets
                    var newStartDate = tweetResult[tweetResult.Count - 1].stamp.ToString("yyyy-MM-ddTHH:mm:ssZ");
                    strGetRequest = "api/v1/Tweets?startDate=" + newStartDate + "&endDate=" + endDate;

                    tweetResult = GetTweetsAsync(strGetRequest).Result;
                    if (tweetResult == null)
                        return null;

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
                return null;
            }

            //Ensure last add tweets are distinct
            filteredtweets = filteredtweets.Distinct<Tweet>(new TweetComparer()).AsParallel().ToList<Tweet>();

            //sw.Stop();
            //var et = sw.ElapsedMilliseconds.ToString();

            return filteredtweets;



        }

        private static async Task<List<Tweet>> GetTweetsAsync(string strGetRequest)
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

        static (bool, DateTime, DateTime) AreDatesValid(string start, string end)
        {
            var startDate = new DateTime();
            if (!DateTime.TryParse(start, out startDate))
            {
                Console.WriteLine("Invalid start date");
                return (false, new DateTime(), new DateTime());
            }

            var endDate = new DateTime();
            if (!DateTime.TryParse(end, out endDate))
            {
                Console.WriteLine("Invalid end date");
                return (false, new DateTime(), new DateTime());
            }

            if (DateTime.Compare(startDate, endDate) > 0)
            {
                Console.WriteLine("Start date cannot be after the end date.");
                return (false, new DateTime(), new DateTime());
            }

            if (DateTime.Compare(startDate, endDate) == 0)
            {
                Console.WriteLine("Start date and end date cannot be the same value.");
                return (false, new DateTime(), new DateTime());
            }

            return (true, startDate, endDate);
        }

        //Retrieve the dates from the user or quit if user presses q
        static (bool, DateTime, DateTime) GetDatesFromUser()
        {
            DateTime validStartDate = new DateTime(0);
            DateTime validEndDate = new DateTime(0);

            var DatesValid = false;

            while (!DatesValid)
            {
                Console.WriteLine("Please enter the start date for tweet retrieval (ex. 2016-01-01T00:00:00, note: time is optional). Press q to quit");
                var startDate = Console.ReadLine();

                if (startDate.ToLower() == "q")
                    return (true, validStartDate, validEndDate);

                Console.WriteLine("Please enter the end date for tweet retrieval (ex. 2018-01-01T00:00:00, note: time is optional). Press q to quit");
                var endDate = Console.ReadLine();

                if (endDate.ToLower() == "q")
                    return (true, validStartDate, validEndDate);

                (DatesValid, validStartDate, validEndDate) = AreDatesValid(startDate, endDate);

            }

            return (false, validStartDate, validEndDate);
        }
    }
}
