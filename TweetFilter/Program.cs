using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace TestConnect
{
    public class Tweet
    {
        public string id { get; set; }
        public DateTime stamp { get; set; }
        public string text { get; set; }

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
    }

    class Program
    {
        static void Main(string[] args)
        {
            Task t = new Task(DownloadPageAsync);
            t.Start();
            Console.WriteLine("Downloading tweets...");
            Console.ReadLine();
        }


        static async void DownloadPageAsync()
        {
            //Stopwatch sw = new Stopwatch();
            //sw.Start();

            List<Tweet> filteredtweets = new List<Tweet>();
            List<Tweet> tweets = new List<Tweet>();

            int nSumUnique = 0;
            // ... Target page.
            string page = "https://badapi.Iqvia.io/";

            string strNewStartDate = "2016-01-01T00:00:00";
            string strEndDate = "2018-01-01T00:00:00";
            string strGetRequest = "api/v1/Tweets?startDate=" + strNewStartDate + "&endDate=" + strEndDate;

            // ... Use HttpClient.
            using (HttpClient client = new HttpClient())
            {
                client.BaseAddress = new Uri(page);
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                try
                {
                    HttpResponseMessage response = await client.GetAsync(strGetRequest);
                    HttpContent content = response.Content;

                    // ... Read the string.
                    string result = await content.ReadAsStringAsync();

                    Newtonsoft.Json.JsonSerializerSettings js = new Newtonsoft.Json.JsonSerializerSettings()
                    {
                        DateTimeZoneHandling = Newtonsoft.Json.DateTimeZoneHandling.Utc
                    };

                    var tweetResult = Newtonsoft.Json.JsonConvert.DeserializeObject<List<Tweet>>(result, js);

                    var tweetResultDistinct = tweetResult.Distinct<Tweet>(new Tweet.TweetComparer()).AsParallel().ToList<Tweet>();

                    filteredtweets = tweetResultDistinct.ToList();


                    DateTime finalDT = new DateTime(2017, 1, 1, 0, 0, 0);
                    while (tweetResultDistinct[tweetResultDistinct.Count - 1].stamp <= finalDT)
                    {
                        strNewStartDate = tweetResultDistinct[tweetResultDistinct.Count - 1].stamp.ToString("yyyy-MM-ddTHH:mm:ssZ");
                        strGetRequest = "api/v1/Tweets?startDate=" + strNewStartDate + "&endDate=" + strEndDate;

                        response = await client.GetAsync(strGetRequest);
                        content = response.Content;

                        // ... Read the string.
                        string result2 = await content.ReadAsStringAsync();

                        tweetResult = Newtonsoft.Json.JsonConvert.DeserializeObject<List<Tweet>>(result2, js);

                        //Avoid duplicates in the get
                        tweetResultDistinct = tweetResult.Distinct<Tweet>(new Tweet.TweetComparer()).AsParallel().ToList<Tweet>();
                        
                        foreach (var tweet in tweetResultDistinct)
                        {
                            if (!filteredtweets.Contains(tweet, new Tweet.TweetComparer()))
                                filteredtweets.Add(tweet);
                        }

                        if (tweetResult.Count < 100) //We have reached the final group of tweets within the date range
                            break;
                    }


                }
                catch (Exception e)
                {
                    string error = e.ToString();
                    Console.WriteLine(error);
                }
            }


            // sw.Stop();
            // var time = sw.ElapsedMilliseconds;

            // ... Display the result.
            foreach (var tweet in filteredtweets)
            {

                Console.WriteLine("ID: {0}, {1}", tweet.id, tweet.stamp);
                Console.WriteLine("Tweet: {0}\n", tweet.text);
            }

            Console.WriteLine("Total number of distinct tweets in 2016 and 2017 is {0}", filteredtweets.Count);



        }
    }
}
