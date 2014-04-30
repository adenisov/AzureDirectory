
using Lucene.Net.Store;

using Microsoft.WindowsAzure.Storage.Blob;

using System;
using System.IO;

namespace AzureDirectory
{
	public class AzureIndexInput : IndexInput
	{
		public AzureIndexInput(AzureDirectory directory, CloudBlockBlob blob)
		{
			if (directory == null)
			{
				throw new ArgumentNullException("directory");
			}

			_stream = new MemoryStream();

			blob.DownloadToStream(_stream);

			_stream.Seek(0, SeekOrigin.Begin);
		}

		#region IndexInput Implementation

		public override byte ReadByte()
		{
			return (byte) _stream.ReadByte();
		}

		public override void ReadBytes(byte[] b, int offset, int len)
		{
			_stream.Read(b, offset, len);
		}

		protected override void Dispose(bool disposing)
		{
		}

		public override void Seek(long pos)
		{
			_stream.Seek(pos, SeekOrigin.Begin);
		}

		public override long Length()
		{
			return _stream.Length;
		}

		public override long FilePointer
		{
			get { return _stream.Position; }
		}

		#endregion

		private readonly MemoryStream _stream;
	}
}
