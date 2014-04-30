
using Lucene.Net.Store;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace AzureDirectory
{
	public class AzureLock : Lock
	{
		public AzureLock(AzureDirectory directory, string lockFile)
		{
			if (directory == null)
			{
				throw new ArgumentNullException("directory");
			}

			_azureDirectory = directory;
			_lockFile = lockFile;
		}

		#region Lock Implementation

		public override bool Obtain()
		{
			var blob = _azureDirectory.BlobDirectory.GetBlockBlobReference(_lockFile);

			try
			{
				if (string.IsNullOrEmpty(_leaseId))
				{
					_leaseId = blob.AcquireLease(TimeSpan.FromSeconds(60), _leaseId);

					_renewTimer = new Timer((obj) =>
					{
						try
						{
							var azureLock = (AzureLock) obj;
							azureLock.Renew();
						}
						catch (Exception e)
						{
							Debug.Write(e, "Lease timer job");
						}
					}, this, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
				}

				return !string.IsNullOrEmpty(_leaseId);
			}
			catch (StorageException e)
			{
				if (CreateLockFile(blob, e))
				{
					return Obtain();
				}
			}
			catch (Exception)
			{
				throw;
			}
		}

		public void Renew()
		{
			if (!string.IsNullOrEmpty(_leaseId))
			{
				var blob = _azureDirectory.BlobDirectory.GetBlockBlobReference(_lockFile);

				blob.RenewLease(new AccessCondition {LeaseId = _leaseId});
			}
		}

		public override void Release()
		{
			if (!string.IsNullOrEmpty(_leaseId))
			{
				var blob = _azureDirectory.BlobDirectory.GetBlockBlobReference(_lockFile);

				blob.ReleaseLease(new AccessCondition {LeaseId = _leaseId});

				if (_renewTimer != null)
				{
					_renewTimer.Dispose();
					_renewTimer = null;
				}

				_leaseId = null;
			}
		}

		public override bool IsLocked()
		{
			return !string.IsNullOrEmpty(_leaseId);
		}

		#endregion

		private bool CreateLockFile(ICloudBlob blob, StorageException e)
		{
			if (e.HResult == 404 || e.HResult == 409)
			{
				using (var stream = new MemoryStream())
				{
					using (var writer = new StreamWriter(stream))
					{
						writer.Write(_lockFile);
						blob.UploadFromStream(stream);
					}
				}

				return true;
			}

			return false;
		}

		public override string ToString()
		{
			return string.Format("AzureLock@{0}.{1}", _lockFile, _leaseId);
		}

		private readonly AzureDirectory _azureDirectory;
		private readonly string _lockFile;

		private string _leaseId;

		private Timer _renewTimer;
	}
}
