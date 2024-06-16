use std::io::{Error, ErrorKind, Result};
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;

use byteorder::{BigEndian, ByteOrder};
use memmap2::MmapMut;
use tokio::fs::File;
use tokio::sync::Mutex;

const OFF_WIDTH: u64 = 4;
const POS_WIDTH: u64 = 8;
const ENT_WIDTH: u64 = OFF_WIDTH + POS_WIDTH;

#[derive(Debug)]
struct InnerIndex {
    file: File,
    mmap: MmapMut,
    size: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SegmentConfig {
    pub(crate) max_index_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Config {
    pub(crate) segment: SegmentConfig,
}

impl InnerIndex {
    async fn new(file: File, config: Config) -> Result<Self> {
        let size = file.metadata().await?.size();
        file.set_len(config.segment.max_index_bytes).await?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self { file, mmap, size })
    }

    async fn read(&self, input: i64) -> Result<(u32, u64)> {
        if self.size == 0 {
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }

        let out = if input == -1 {
            (self.size / ENT_WIDTH) - 1
        } else {
            input as u64
        };

        let pos = ENT_WIDTH * out;

        if self.size < pos + ENT_WIDTH {
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }

        let out = BigEndian::read_u32(&self.mmap[pos as usize..(pos + OFF_WIDTH) as usize]);
        let pos = BigEndian::read_u64(&self.mmap[(pos + OFF_WIDTH) as usize..(pos + ENT_WIDTH) as usize]);

        Ok((out, pos))
    }

    async fn write(&mut self, off: u32, pos: u64) -> Result<()> {
        if self.mmap.len() < (self.size + ENT_WIDTH) as usize {
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }

        BigEndian::write_u32(
            &mut self.mmap[self.size as usize..(self.size + OFF_WIDTH) as usize],
            off,
        );
        BigEndian::write_u64(
            &mut self.mmap[(self.size + OFF_WIDTH) as usize..(self.size + ENT_WIDTH) as usize],
            pos,
        );
        self.size += ENT_WIDTH;

        Ok(())
    }

    async fn finish(&self) -> Result<()> {
        self.mmap.flush()?;
        self.file.set_len(self.size).await?;

        Ok(())
    }
}

pub(crate) struct Index {
    inner: Arc<Mutex<InnerIndex>>,
}

impl Index {
    pub(crate) async fn new(file: File, config: Config) -> Result<Self> {
        let inner_index = InnerIndex::new(file, config).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner_index))
        })
    }

    pub(crate) async fn read(&self, input: i64) -> Result<(u32, u64)> {
        let inner_guard = self.inner.lock().await;
        inner_guard.read(input).await
    }

    pub(crate) async fn write(&self, off: u32, pos: u64) -> Result<()> {
        let mut inner_guard = self.inner.lock().await;
        inner_guard.write(off, pos).await
    }

    pub(crate) async fn close(self) -> Result<()> {
        let inner_guard = self.inner.lock().await;
        inner_guard.finish().await
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use tokio::fs;
    use tokio::fs::File;

    use crate::log::index::{Config, Index};

    #[tokio::test]
    async fn test_index() {
        let file_path = env::temp_dir().join("index_test");
        let t_file = File::options().create(true).read(true).write(true).open(&file_path).await.unwrap();

        let mut config = Config::default();
        config.segment.max_index_bytes = 1024;

        let idx = Index::new(t_file, config.clone()).await.unwrap();
        let res = idx.read(-1).await;
        res.expect_err("expected unexpected EOF error");

        let entries = [(0, 0), (1, 10)];

        for entry in entries {
            let (w_off, w_pos) = entry;
            idx.write(w_off, w_pos).await.unwrap();

            let (_, r_pos) = idx.read(w_off as i64).await.unwrap();
            assert_eq!(w_pos, r_pos)
        }

        let res = idx.read(entries.len() as i64).await;
        res.expect_err("expected unexpected EOF error");
        idx.close().await.unwrap();

        let t_file = File::options().create(true).read(true).write(true).open(&file_path).await.unwrap();
        let idx = Index::new(t_file, config.clone()).await.unwrap();
        let (off, pos) = idx.read(-1).await.unwrap();
        assert_eq!(1, off);
        assert_eq!(entries[1].1, pos);

        idx.close().await.unwrap();
        fs::remove_file(&file_path).await.unwrap();
    }
}