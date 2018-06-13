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

            //using (var connection = new SqlConnection("Server=(LocalDb)\\MSSQLLocalDB;Database=Scratch"))
            //{
            //    var texts = connection.Query<Text>("select * from [Text]");
            //    var result = client.Bulk(b => b.CreateMany(texts).Index("text_idx"));

            //    Console.WriteLine(result.Items.Count);
            //}

            var count = 0;
            foreach (var doc in QueryByContent(client, "prince"))
            {
                Console.WriteLine($"{doc.Index}: {doc.Content}");
                count += 1;
            }
            Console.WriteLine($"Total count: {count}");
        }

        static IEnumerable<Text> QueryByContent(ElasticClient client, string content)
        {
            var initialResult = client.Search<Text>(s =>
            {
                return s.Index("text_idx")
                    .Skip(0)
                    .Size(100)
                    .Query(q => q.Term(t => t.Content, content));
            });

            foreach (var doc in initialResult.Hits.Select(hit => hit.Source))
            {
                yield return doc;
            }

            var viewed = 100;
            var remaining = initialResult.Total - 100;

            while (remaining > 0)
            {
                var result = client.Search<Text>(s =>
                {
                    return s.Index("text_idx")
                        .Skip(viewed)
                        .Size(100)
                        .Query(q => q.Term(t => t.Content, content));
                });

                viewed += result.Hits.Count;
                remaining -= result.Hits.Count;

                foreach (var doc in result.Hits.Select(hit => hit.Source))
                {
                    yield return doc;
                }
            }
        }
    }
}
