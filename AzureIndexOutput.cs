
using Lucene.Net.Store;

using Microsoft.WindowsAzure.Storage.Blob;

using System;
using System.IO;

namespace AzureDirectory
{
	public class AzureIndexOutput : IndexOutput
	{
		public AzureIndexOutput(AzureDirectory directory, CloudBlockBlob blob)
		{
			if (directory == null)
			{
				throw new ArgumentNullException("directory");
			}

			_blob = blob;

			_stream = new MemoryStream();
		}

		#region IndexOutput Implementation
		
		public override void WriteByte(byte b)
		{
			_stream.WriteByte(b);
		}

		public override void WriteBytes(byte[] b, int offset, int length)
		{
			_stream.Write(b, offset, length);
		}

		public override void Flush()
		{
			_stream.Seek(0, SeekOrigin.Begin);

			_blob.UploadFromStream(_stream);
		}

		protected override void Dispose(bool disposing)
		{
			Flush();
		}

		public override void Seek(long pos)
		{
			_stream.Seek(pos, SeekOrigin.Begin);
		}

		public override long FilePointer
		{
			get { return _stream.Position; }
		}

		public override long Length
		{
			get { return _stream.Length; }
		}

		#endregion

		private readonly CloudBlockBlob _blob;
		private readonly MemoryStream _stream;
	}
}
