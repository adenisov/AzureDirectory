
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;

using System;

using Version = Lucene.Net.Util.Version;

namespace AzureDirectory
{
	public static class Program
	{
		public static void Main(string[] args)
		{
			var storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

			var directory = new AzureDirectory(storageAccount, "testontainer", "imageIndex");

			var analyzer = new StandardAnalyzer(Version.LUCENE_30);

			//var indexWriter = new IndexWriter(directory, analyzer, new IndexWriter.MaxFieldLength(2000));

			//var doc = new Document();

			//doc.Add(new Field("id", Guid.NewGuid().ToString(), Field.Store.YES, Field.Index.NO));
			//doc.Add(new Field("keywords", "nissan, teana, j32, car", Field.Store.YES, Field.Index.ANALYZED));

			//indexWriter.AddDocument(doc);
			//indexWriter.Commit();

			var queryParser = new QueryParser(Version.LUCENE_30, "keywords", analyzer);

			var q = queryParser.Parse("teana");

			var searcher = new IndexSearcher(directory);

			var topDocs = searcher.Search(q, 200);

			var total = topDocs.TotalHits;

			for (var i = 0; i < total; i++)
			{
				var d = searcher.Doc(topDocs.ScoreDocs[i].Doc);
			}
		}
	}
}
