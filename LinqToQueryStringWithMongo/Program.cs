using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using LinqToQuerystring;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace LinqToQueryStringWithMongo
{
    /// <summary>
    /// Goal: use odata-like querystring to filter a dynamic collection in mongodb (2.6.7)
    /// </summary>
    /// <remarks>
    /// based upon http://linqtoquerystring.net/examples.html
    /// </remarks>
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("Start execution");

            // setup mongo connection
            const string connectionString = "mongodb://localhost";
            var client = new MongoClient(connectionString);
            var server = client.GetServer();
            var database = server.GetDatabase("sampledb");

            // get collection of type BsonDocument (MongoDB.Driver's  equivalent of a dictionary)
            var mongoCollection = database.GetCollection<BsonDocument>("Dynamic");
 
            // create/populate collection
            InsertRandomRecords(mongoCollection, 1000000 /* 1mil */);

            var sw = Stopwatch.StartNew();

            // note: [Age] instead of Age -> square brackets are necessary
            var result = mongoCollection.AsQueryable().LinqToQuerystring("$filter=[Age] eq 36").ToList();

            sw.Stop();
            Console.WriteLine("Query completed in {0}ms", sw.ElapsedMilliseconds);
            // averages over a few trials with 1,000,000 records:
            // with index:    240ms
            // without index: 700ms

            Console.WriteLine("Execution finished");
            Console.Read();
        }

        private static void InsertRandomRecords(MongoCollection<BsonDocument> mongoCollection, int count)
        {
            Console.WriteLine("Inserting {0} records", count);
            const int batchsize = 50000;
            var rnd = new Random();

            var buffer = new List<BsonDocument>();
            for (int i = 0; i < count; i++)
            {
                buffer.Add(
                    new BsonDocument
                    {
                        {"Name", "A test record"},
                        {"Date", new DateTime(2013, 01, 01)},
                        {"Age", rnd.Next(0, 100)},
                        {"Complete", false}
                    });

                if ((i + 1)%batchsize == 0)
                {
                    mongoCollection.InsertBatch(buffer);
                    buffer = new List<BsonDocument>();
                    Console.WriteLine("Inserted [{0}/{1}]", (i + 1), count);
                }
            }

            if (buffer.Count > 0)
                mongoCollection.InsertBatch(buffer);

            Console.WriteLine("Inserted {0} records", count);
        }
    }
}