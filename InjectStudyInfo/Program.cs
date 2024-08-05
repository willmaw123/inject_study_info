using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using Newtonsoft;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace InjectStudyInfo
{
    class Program
    {
		static string key = "";
        static string sourceFile;
        static string errorFile;
        static bool stopOnEach;
        static string elastichost;
        static string cassandraHost;
        static int delay;
        static void Main(string[] args)
        {
			key = Environment.GetEnvironmentVariable("VITAL_KEY");
			
            bool direct = false;

            if (args.Length < 2)
            {
                Console.WriteLine("usage: InjectStudyInfo.exe -direct -elastichost [elastic host ip] -srcfile [srcfile] -errorfile [errorFile] -cassandrahost [cassandrahost ip] -stopOnEach -delay [ms]" );
                Environment.Exit(0);
            }

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].ToLower() == "-direct")
                {
                    direct = true;
                }

                if (args[i].ToLower() == "-delay")
                {
                    delay = int.Parse(args[i + 1]);
                }

                if (args[i].ToLower() == "-elastichost")
                {
                    elastichost = args[i + 1];
                }

                if (args[i].ToLower() == "-srcfile")
                {
                    sourceFile = args[i + 1];
                }

                if (args[i].ToLower() == "-stoponeach")
                {
                    stopOnEach = true;
                }

                if (args[i].ToLower() == "-errorfile")
                {
                    errorFile = args[i + 1];
                }

                if (args[i].ToLower() == "-cassandrahost")
                {
                    cassandraHost = args[i + 1];
                }
            }

            if (direct)
            {
                StartDirect().GetAwaiter().GetResult();
            }
            else
            {
                Start().GetAwaiter().GetResult();
            }

        }

        private static async Task StartDirect()
        {
            await RecreateIndex();

            var cluster = Cluster.Builder()
                                 .AddContactPoints(cassandraHost)
                                 .Build();

            var session = cluster.Connect("imagearchive");
            Console.WriteLine("Starting cassandra query");
            var rs = session.Execute("SELECT study_instance_uid, timeline_event FROM new_study");
            Console.WriteLine("Cassandra query complete");
            foreach (var row in rs)
            {
                var study_uid = row.GetValue<string>("study_instance_uid");
                var timeline_event = row.GetValue<string>("timeline_event");

                Console.WriteLine($"Processing : {study_uid}" );

                // send 
                await SendData(study_uid, timeline_event);

                if (stopOnEach)
                {
                    Console.WriteLine("Press a key to send the next one...");
                    Console.ReadKey();
                }
            }
        }

        private static async Task<bool> RecreateIndex()
        {
            Console.WriteLine($"elastic host is {elastichost}");

            try
            {
                using (var httpClientHandler = new HttpClientHandler() { Credentials = new NetworkCredential("vital", key) })
                {
                    httpClientHandler.ServerCertificateCustomValidationCallback = (message, cert, chain, sslPolicyErrors) =>
                    {
                        if (sslPolicyErrors == System.Net.Security.SslPolicyErrors.None)
                        {
                            return true;
                        }

                        return true;
                    };

                    using (var httpclient = new HttpClient(httpClientHandler))
                    {
                        var rsp = await httpclient.DeleteAsync($"https://{elastichost}:9200/vitreastudyinfo/");

                        if (rsp.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"successfully deleted index");
                        }

                        var rsp2 = await httpclient.PutAsync($"https://{elastichost}:9200/vitreastudyinfo/", null);

                        if (rsp2.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"successfully recreated index");
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex}");
                return false;
            }
        }

        private static async Task SendData(string studyuid, string timelineEvent)
        {
            JObject jobject = null;
            Console.WriteLine($"Sending data for {studyuid}");
            try
            {
                jobject = JObject.Parse(timelineEvent);
            }
            catch (Exception)
            {
                Console.WriteLine($"error parsing json string for study {studyuid}");
                File.AppendAllLines(errorFile, new string[] { studyuid });
                return;
            }

            jobject.Add("studyinstanceuid", studyuid);
            jobject.Remove("id");
            string rqbody = jobject.ToString();
            var data = Encoding.ASCII.GetBytes(rqbody);

            try
            {
                using (var httpClientHandler = new HttpClientHandler() { Credentials = new NetworkCredential("vital", key) })
                {
                    httpClientHandler.ServerCertificateCustomValidationCallback = (message, cert, chain, sslPolicyErrors) =>
                    {
                        if (sslPolicyErrors == System.Net.Security.SslPolicyErrors.None)
                        {
                            return true;
                        }

                        return true;
                    };

                    using (var httpclient = new HttpClient(httpClientHandler))
                    {
                        httpclient.Timeout = new TimeSpan(0, 0, 5);
                        HttpContent content = new StringContent(rqbody);
                        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                        var rsp = await httpclient.PostAsync($"https://{elastichost}:9200/vitreastudyinfo/_doc", content);

                        if (rsp.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"successfully sent : {studyuid}");
                        }
                        else
                        {
                            File.AppendAllLines(errorFile, new string[] { studyuid });
                        }

                        httpclient.
                    }
                }

                await Task.Delay(delay);
            }
            catch (Exception ex)
            {
                File.AppendAllLines(errorFile, new string[] { studyuid });
                Console.WriteLine($"{ex}");
            }
        }

        private static async Task Start()
        {
            await RecreateIndex();

            using (StreamReader rdr = new StreamReader(sourceFile))
            {
                string line = "";
                int linenum = 1;
                while ((line = rdr.ReadLine()) != null)
                {
                    if (line.Trim().Length == 0)
                    {
                        continue;
                    }

                    var columns = line.Split('|');

                    if (columns[0].Contains("study_instance_uid") || columns[0].Contains("------"))
                    {
                        continue;
                    }

                    string studyuid = columns[0].Trim();

                    string timelineEvent = columns[1].Trim();


                    await SendData(studyuid, timelineEvent);

                    if (stopOnEach)
                    {
                        Console.WriteLine("Press a key to send the next one...");
                        Console.ReadKey();
                    }

                    linenum++;
                }

            }

            return;
        }
    }
}
