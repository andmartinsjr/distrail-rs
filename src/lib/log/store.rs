use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, Result};
use tokio::sync::Mutex;

const LEN_WIDTH: u64 = 8;
const BUFFER_SIZE: usize = 8 * 1024;

#[derive(Debug)]
struct InnerStore {
    file: File,
    buffer: Vec<u8>,
    size: u64,
}

impl InnerStore {
    async fn new(file: File) -> Result<Self> {
        let size = file.metadata().await?.size();
        let buffer = Vec::with_capacity(BUFFER_SIZE);

        Ok(Self { file, size, buffer })
    }

    async fn append(&mut self, p: &[u8]) -> Result<(u64, u64)> {
        let pos = self.size;
        let len = p.len() as u64;

        self.buffer.write_u64(len).await?;
        self.buffer.write_all(p).await?;

        let written = len + LEN_WIDTH;

        self.size += written;

        Ok((written, pos))
    }

    async fn read(&mut self, pos: u64) -> Result<Vec<u8>> {
        self.file.write_all(&self.buffer).await?;
        self.file.flush().await?;
        self.buffer.clear();

        let mut size_buffer = [0_u8; LEN_WIDTH as usize];
        self.file.seek(SeekFrom::Start(pos)).await?;
        self.file.read_exact(&mut size_buffer).await?;
        let size = u64::from_be_bytes(size_buffer);

        let mut buf = vec![0u8; size as usize];
        self.file.seek(SeekFrom::Start(pos + LEN_WIDTH)).await?;
        self.file.read_exact(&mut buf).await?;

        Ok(buf)
    }

    async fn read_at(&mut self, p: &mut [u8], off: u64) -> Result<u64> {
        self.file.write_all(&self.buffer).await?;
        self.file.flush().await?;
        self.buffer.clear();

        self.file.seek(SeekFrom::Start(off)).await?;
        let read_size = self.file.read_exact(p).await?;

        Ok(read_size as u64)
    }

    async fn flush(&mut self) -> Result<()> {
        self.file.write_all(&self.buffer).await?;
        self.file.flush().await?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Store {
    inner_store: Arc<Mutex<InnerStore>>,
}

impl Store {
    pub(crate) async fn new(file: File) -> Result<Self> {
        let inner = InnerStore::new(file).await?;
        Ok(Self {
            inner_store: Arc::new(Mutex::new(inner)),
        })
    }

    pub(crate) async fn append(&self, p: &[u8]) -> Result<(u64, u64)> {
        let mut inner_guard = self.inner_store.lock().await;
        inner_guard.append(p).await
    }

    pub(crate) async fn read(&self, pos: u64) -> Result<Vec<u8>> {
        let mut inner_guard = self.inner_store.lock().await;
        inner_guard.read(pos).await
    }

    pub(crate) async fn read_at(&self, p: &mut [u8], off: u64) -> Result<u64> {
        let mut inner_guard = self.inner_store.lock().await;
        inner_guard.read_at(p, off).await
    }

    pub(crate) async fn close(self) -> Result<()> {
        let mut inner_guard = self.inner_store.lock().await;
        inner_guard.flush().await
    }
}

