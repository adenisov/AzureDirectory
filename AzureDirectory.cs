
using System.Collections.Generic;
using System.IO;
using Lucene.Net.Store;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

using System;
using System.Linq;
using Directory = Lucene.Net.Store.Directory;

namespace AzureDirectory
{
	public class AzureDirectory : Directory
	{
		public AzureDirectory(CloudStorageAccount account, string container, string catalog)
		{
			if (account == null)
			{
				throw new ArgumentNullException("account");
			}

			if (string.IsNullOrEmpty(container))
			{
				container = "_lucene";
			}

			_container = container;

			if (string.IsNullOrEmpty(catalog))
			{
				catalog = "_index";
			}

			_catalog = catalog;

			_storageAccount = account;

			Init();
		}

		public CloudBlobDirectory BlobDirectory
		{
			get { return _blobDirectory; }
		}

		public CloudBlobContainer BlobContainer
		{
			get { return _blobContainer; }
		}

		public CloudBlobClient BlobClient
		{
			get { return _blobClient; }
		}

		private void Init()
		{
			_blobClient = _storageAccount.CreateCloudBlobClient();

			_blobContainer = _blobClient.GetContainerReference(_container);

			_blobContainer.CreateIfNotExists();

			_blobDirectory = _blobContainer.GetDirectoryReference(_catalog);
		}

		#region Directory Implemenation

		public override string[] ListAll()
		{
			var blobs = from blob in BlobDirectory.ListBlobs()
				select blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.LastIndexOf('/') + 1);

			return blobs.ToArray();
		}

		public override bool FileExists(string name)
		{
			try
			{
				return BlobDirectory.GetBlockBlobReference(name).Exists();
			}
			catch (Exception e)
			{
				return false;
			}
		}

		public override long FileModified(string name)
		{
			try
			{
				var blob = BlobDirectory.GetBlockBlobReference(name);

				blob.FetchAttributes();

				return blob.Properties.LastModified != null
					? blob.Properties.LastModified.Value.UtcDateTime.ToFileTimeUtc()
					: 0;
			}
			catch (Exception e)
			{
				return 0;
			}
		}

		public override void TouchFile(string name)
		{
			throw new System.NotImplementedException();
		}

		public override void DeleteFile(string name)
		{
			try
			{
				var blob = BlobDirectory.GetBlockBlobReference(name);

				blob.DeleteIfExists();
			}
			catch (Exception)
			{
				throw;
			}
		}

		public override long FileLength(string name)
		{
			try
			{
				var blob = BlobDirectory.GetBlockBlobReference(name);

				blob.FetchAttributes();

				return blob.Properties.Length;
			}
			catch (Exception)
			{
				throw;
			}
		}

		public override IndexOutput CreateOutput(string name)
		{
			var blob = BlobDirectory.GetBlockBlobReference(name);

			return new AzureIndexOutput(this, blob);
		}

		public override IndexInput OpenInput(string name)
		{
			try
			{
				var blob = BlobDirectory.GetBlockBlobReference(name);

				blob.FetchAttributes();

				return new AzureIndexInput(this, blob);
			}
			catch (Exception e)
			{
				throw new FileNotFoundException(name, e);
			}
		}

		public override Lock MakeLock(string name)
		{
			lock (_locks)
			{
				var azureLock = new AzureLock(this, name);

				_locks.Add(name, azureLock);

				return azureLock;
			}
		}

		public override void ClearLock(string name)
		{
			lock (_locks)
			{
				if (_locks.ContainsKey(name))
				{
					_locks[name].Release();
				}
			}
		}

		protected override void Dispose(bool disposing)
		{
			_blobDirectory = null;
			_blobContainer = null;
			_blobClient = null;
		}

		#endregion

		private readonly CloudStorageAccount _storageAccount;
		private readonly string _container;
		private readonly string _catalog;

		private readonly IDictionary<string, AzureLock> _locks = new Dictionary<string, AzureLock>();

		private CloudBlobClient _blobClient;
		private CloudBlobContainer _blobContainer;
		private CloudBlobDirectory _blobDirectory;
	}
}
