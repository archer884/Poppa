using Dapper;
using Nest;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Poppa
{
    class Program
    {
        const string IndexName = "text_index";

        class Text
        {
            public int Id { get; set; }
            public int Index { get; set; }
            public int Length { get; set; }
            public int WordCount { get; set; }
            public string Content { get; set; }
        }

        static void Main(string[] args)
        {
            var serverAddress = new Uri("http://localhost:9200");
            var settings = new ConnectionSettings(serverAddress);
            var client = new ElasticClient(settings);

            // Uncomment to repopulate elasticsearch.
            //using (var connection = new SqlConnection("Server=(LocalDb)\\MSSQLLocalDB;Database=Scratch"))
            //{
            //    var texts = connection.Query<Text>("select * from [Text]");
            //    var result = client.Bulk(create => create
            //        .Index(IndexName)
            //        .CreateMany(texts, (op, text) => op.Id(text.Id)));

            //    Console.WriteLine(result.Items.Count);
            //}

            // Uncomment to perform a search.
            //if (args.Length != 1)
            //{
            //    throw new ArgumentException("Caller must provide one search term.");
            //}

            //var count = 0;
            //foreach (var doc in QueryByContent(client, args[0]))
            //{
            //    if (doc.Id != doc.Source.Id.ToString())
            //        throw new Exception("Id mismatch");

            //    Console.WriteLine($"{doc.Source.Index}: {doc.Source.Content}");
            //    count += 1;
            //}
            //Console.WriteLine($"Total count: {count}");
        }

        static IEnumerable<IHit<Text>> QueryByContent(ElasticClient client, string content)
        {
            var initialResult = client.Search<Text>(s =>
            {
                return s.Index(IndexName)
                    .Skip(0)
                    .Size(100)
                    .Query(q => q.Term(t => t.Content, content));
            });

            Console.WriteLine($"Returning {initialResult.Total} results.");
            foreach (var hit in initialResult.Hits)
            {
                yield return hit;
            }

            var viewed = 100;
            var remaining = initialResult.Total - 100;

            while (remaining > 0)
            {
                var result = client.Search<Text>(s =>
                {
                    return s.Index(IndexName)
                        .Skip(viewed)
                        .Size(100)
                        .Query(q => q.Term(t => t.Content, content));
                });

                viewed += result.Hits.Count;
                remaining -= result.Hits.Count;

                foreach (var hit in result.Hits)
                {
                    yield return hit;
                }
            }
        }
    }
}
