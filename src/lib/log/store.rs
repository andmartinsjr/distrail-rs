use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;

use byteorder::{BigEndian, ByteOrder};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter, Result};
use tokio::sync::Mutex;

const LEN_WIDTH: u64 = 8;

#[derive(Debug)]
struct InnerStore {
    buf_file: BufWriter<File>,
    size: u64,
}

impl InnerStore {
    async fn new(file: File) -> Result<Self> {
        let size = file.metadata().await?.size();
        let buf_file = BufWriter::new(file);

        Ok(Self { buf_file, size })
    }

    async fn append(&mut self, p: &[u8]) -> Result<(u64, u64)> {
        let pos = self.size;
        let len = p.len() as u64;

        self.buf_file.write_u64(len).await?;
        self.buf_file.write_all(p).await?;

        let written = len + LEN_WIDTH;

        self.size += written;

        Ok((written, pos))
    }

    async fn read(&mut self, pos: u64) -> Result<Vec<u8>> {
        self.buf_file.flush().await?;

        let mut size_buffer = [0_u8; LEN_WIDTH as usize];
        self.buf_file.seek(SeekFrom::Start(pos)).await?;
        self.buf_file.read_exact(&mut size_buffer).await?;
        let size = BigEndian::read_u64(&size_buffer);

        let mut buf = vec![0u8; size as usize];
        self.buf_file.seek(SeekFrom::Start(pos + LEN_WIDTH)).await?;
        self.buf_file.read_exact(&mut buf).await?;

        Ok(buf)
    }

    async fn read_at(&mut self, p: &mut [u8], off: u64) -> Result<u64> {
        self.buf_file.flush().await?;

        self.buf_file.seek(SeekFrom::Start(off)).await?;
        let read_size = self.buf_file.read_exact(p).await?;

        Ok(read_size as u64)
    }

    async fn flush(&mut self) -> Result<()> {
        self.buf_file.flush().await?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Store {
    inner: Arc<Mutex<InnerStore>>,
}

impl Store {
    pub(crate) async fn new(file: File) -> Result<Self> {
        let inner = InnerStore::new(file).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub(crate) async fn append(&self, p: &[u8]) -> Result<(u64, u64)> {
        let mut inner_guard = self.inner.lock().await;
        inner_guard.append(p).await
    }

    pub(crate) async fn read(&self, pos: u64) -> Result<Vec<u8>> {
        let mut inner_guard = self.inner.lock().await;
        inner_guard.read(pos).await
    }

    pub(crate) async fn read_at(&self, p: &mut [u8], off: u64) -> Result<u64> {
        let mut inner_guard = self.inner.lock().await;
        inner_guard.read_at(p, off).await
    }

    pub(crate) async fn close(self) -> Result<()> {
        let mut inner_guard = self.inner.lock().await;
        inner_guard.flush().await
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::os::unix::fs::MetadataExt;

    use byteorder::{BigEndian, ByteOrder};
    use tokio::fs::{self, File};

    use super::{LEN_WIDTH, Store};

    const TEST_LOG_ENTRY: &[u8] = b"hello world";
    const WIDTH: u64 = TEST_LOG_ENTRY.len() as u64 + LEN_WIDTH;

    #[tokio::test]
    async fn store_append_read() {
        let tmp_dir = env::temp_dir();
        let file_path = tmp_dir.join("store_append_read_test");
        let t_file = File::options().append(true).read(true).create(true).open(&file_path).await.unwrap();
        let t_store = Store::new(t_file).await.unwrap();
        test_append(&t_store).await;
        test_read(&t_store).await;
        test_read_at(&t_store).await;
        drop(t_store);
        let t_file = File::options().append(true).read(true).create(true).open(&file_path).await.unwrap();
        let t_store = Store::new(t_file).await.unwrap();
        test_read(&t_store).await;
        drop(t_store);
        fs::remove_file(file_path).await.unwrap();
    }

    async fn test_append(s: &Store) {
        for i in 1..4 {
            let (n, pos) = s.append(TEST_LOG_ENTRY).await.unwrap();
            assert_eq!(pos + n, WIDTH * i)
        }
    }

    async fn test_read(s: &Store) {
        let mut pos = 0;
        for _ in 1..4 {
            let read = s.read(pos).await.unwrap();
            assert_eq!(TEST_LOG_ENTRY, read);
            pos += WIDTH
        }
    }

    async fn test_read_at(s: &Store) {
        let mut off = 0;
        for _ in 1..4 {
            let mut size_buf = [0_u8; LEN_WIDTH as usize];
            let n = s.read_at(&mut size_buf, off).await.unwrap();
            assert_eq!(LEN_WIDTH, n);
            off += n;
            let size = BigEndian::read_u64(&size_buf);
            let mut buf = vec![0u8; size as usize];
            let n = s.read_at(&mut buf, off).await.unwrap();
            assert_eq!(TEST_LOG_ENTRY, buf);
            assert_eq!(size, n);
            off += n
        }
    }

    #[tokio::test]
    async fn store_close() {
        let tmp_dir = env::temp_dir();
        let file_path = tmp_dir.join("store_close_test");
        let t_file = File::options().append(true).read(true).create(true).open(&file_path).await.unwrap();
        let t_store = Store::new(t_file).await.unwrap();
        t_store.append(TEST_LOG_ENTRY).await.unwrap();
        let before_size = get_file_size().await.unwrap();
        t_store.close().await.unwrap();
        let after_size = get_file_size().await.unwrap();
        assert!(after_size > before_size);
        fs::remove_file(file_path).await.unwrap();
    }

    async fn get_file_size() -> std::io::Result<u64> {
        let tmp_dir = env::temp_dir();
        let file_path = tmp_dir.join("store_close_test");
        let t_file = File::options().append(true).read(true).create(true).open(&file_path).await?;

        let file_size = t_file.metadata().await?.size();

        Ok(file_size)
    }
}
