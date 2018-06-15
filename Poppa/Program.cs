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
        const string DbConnectionString = "Server=(LocalDb)\\MSSQLLocalDB;Database=Scratch";
        const string IndexName = "text_index";
        const string TextQuery = "select * from [Text]";
        const string LeftToken = "2534d2e2-8340-4f67-974e-7dd4564ac1d5";
        const string RightToken = "65b3860f-e205-44d9-a7e0-aade6db53f30";

        class Text
        {
            public int Id { get; set; }
            public int Index { get; set; }
            public int Length { get; set; }
            public int WordCount { get; set; }
            public string Content { get; set; }
            public string Token { get; set; }
        }

        enum Mode
        {
            Invalid,
            Populate,
            Query,
            Update,
        }

        static void Main(string[] args)
        {
            var serverAddress = new Uri("http://localhost:9200");
            var settings = new ConnectionSettings(serverAddress);
            var client = new ElasticClient(settings);

            switch (GetMode(args))
            {
                case Mode.Invalid:
                    Console.WriteLine("Error: please select a valid mode");
                    return;

                case Mode.Populate:
                    Console.WriteLine("Populate Elasticsearch");
                    Console.WriteLine($"Added {Populate(client)} items.");
                    return;

                case Mode.Query:
                    Console.WriteLine($"Query Elasticsearch: {args[1]}");
                    var count = 0;
                    foreach (var doc in QueryByContent(client, args[1]))
                    {
                        Console.WriteLine($"{doc.Source.Index}: {doc.Source.Content}");
                        count += 1;
                    }
                    Console.WriteLine($"Total result count: {count}");
                    return;

                case Mode.Update:
                    Console.WriteLine("Update Elasticsearch");
                    using (var connection = new SqlConnection(DbConnectionString))
                    {
                        var texts = connection.Query<Text>(TextQuery).Select(text => new Text
                        {
                            Id = text.Id,
                            Index = text.Index,
                            Length = text.Length,
                            WordCount = text.WordCount,
                            Content = text.Content.Replace("prince", "gnbht", StringComparison.InvariantCultureIgnoreCase),
                        });

                        var result = client.Bulk(bulk => bulk
                            .Index(IndexName)
                            .UpdateMany(texts, (descriptor, text) => descriptor.Id(text.Id).Doc(text).DocAsUpsert()));
                    }
                    return;
            }
        }

        static int Populate(ElasticClient client)
        {
            Text WithToken(Text text, int idx)
            {
                return new Text
                {
                    Id = text.Id,
                    Index = text.Index,
                    Length = text.Length,
                    WordCount = text.WordCount,
                    Content = text.Content,
                    Token = idx % 2 == 0 ? LeftToken : RightToken,
                };
            }

            using (var connection = new SqlConnection(DbConnectionString))
            {
                var texts = connection.Query<Text>(TextQuery).Select(WithToken);
                var result = client.Bulk(bulk => bulk
                    .Index(IndexName)
                    .CreateMany(texts, (op, text) => op.Id(text.Id)));

                return result.Items.Count;
            }
        }

        static IEnumerable<IHit<Text>> QueryByContent(ElasticClient client, string content)
        {
            var initialResult = client.Search<Text>(s =>
            {
                return s.Index(IndexName)
                    .Skip(0)
                    .Size(100)
                    .Query(q => q.Term(t => t.Content, content))
                    .Query(q => q.Term(t => t.Token, LeftToken));
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
                        .Query(q => q.Term(t => t.Content, content))
                        .Query(q => q.Term(t => t.Token, LeftToken));
                });

                viewed += result.Hits.Count;
                remaining -= result.Hits.Count;

                foreach (var hit in result.Hits)
                {
                    yield return hit;
                }
            }
        }

        static Mode GetMode(string[] args)
        {
            switch (args.Length)
            {
                case 1:
                    switch (args[0])
                    {
                        case "populate": return Mode.Populate;
                        case "update": return Mode.Update;
                        default: return Mode.Invalid;
                    }

                case 2:
                    if ("query" == args[0])
                    {
                        return Mode.Query;
                    }
                    else
                    {
                        return Mode.Invalid;
                    }

                default: return Mode.Invalid;
            }
        }
    }
}
