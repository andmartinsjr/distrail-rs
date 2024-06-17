use std::io::Result;
use std::path::Path;
use std::sync::Arc;

use prost::Message;
use tokio::fs::File;
use tokio::sync::Mutex;

use crate::log::config::Config;
use crate::log::index::Index;
use crate::log::store::Store;
use crate::log_v1::Record;

#[derive(Debug)]
struct InnerSegment {
    store: Store,
    index: Index,
    base_offset: u64,
    next_offset: u64,
    config: Config,
}

impl InnerSegment {
    async fn new(dir: impl AsRef<Path>, base_offset: u64, config: Config) -> Result<Self> {
        let store_file_name = format!("{base_offset}.store");
        let store_file = File::options().create(true).read(true).append(true).open(dir.as_ref().join(store_file_name)).await?;
        let store = Store::new(store_file).await?;

        let index_file_name = format!("{base_offset}.index");
        let index_file = File::options().create(true).read(true).write(true).open(dir.as_ref().join(index_file_name)).await?;
        let index = Index::new(index_file, &config).await?;

        let next_offset = match index.read(-1).await {
            Ok((off, _)) => base_offset + off as u64 + 1,
            Err(_) => base_offset,
        };

        Ok(Self {
            config,
            store,
            index,
            next_offset,
            base_offset,
        })
    }

    async fn append(&mut self, record: &mut Record) -> Result<u64> {
        let cur = self.next_offset;
        record.offset = cur;

        let mut encoded_record_buf = vec![];
        record.encode(&mut encoded_record_buf)?;

        let (_, pos) = self.store.append(&encoded_record_buf).await?;

        self.index.write((self.next_offset - self.base_offset) as u32, pos).await?;

        self.next_offset += 1;

        Ok(cur)
    }

    async fn read(&mut self, off: u64) -> Result<Record> {
        let (_, pos) = self.index.read((off - self.base_offset) as i64).await?;

        let p = self.store.read(pos).await?;
        let record = Record::decode(&p[..])?;

        Ok(record)
    }

    async fn is_maxed(&self) -> bool {
        self.store.size().await >= self.config.segment.max_store_bytes || self.index.size().await >= self.config.segment.max_index_bytes
    }

    async fn finish(&self) -> Result<()> {
        self.store.finish().await?;
        self.index.finish().await?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Segment {
    inner: Arc<Mutex<InnerSegment>>,
}

impl Segment {
    pub(crate) async fn new(dir: &Path, base_offset: u64, config: Config) -> Result<Self> {
        let inner_segment = InnerSegment::new(dir, base_offset, config).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner_segment)),
        })
    }

    pub(crate) async fn append(&self, record: &mut Record) -> Result<u64> {
        let mut inner_guard = self.inner.lock().await;
        inner_guard.append(record).await
    }

    pub(crate) async fn read(&self, off: u64) -> Result<Record> {
        let mut inner_guard = self.inner.lock().await;
        inner_guard.read(off).await
    }

    pub(crate) async fn is_maxed(&self) -> bool {
        let inner_guard = self.inner.lock().await;
        inner_guard.is_maxed().await
    }

    pub(crate) async fn next_offset(&self) -> u64 {
        self.inner.lock().await.next_offset
    }

    pub(crate) async fn close(self) -> Result<()> {
        let inner_guard = self.inner.lock().await;
        inner_guard.finish().await
    }

    pub(crate) async fn remove(self) -> Result<()> {
        self.close().await?;
        //TODO: remove files
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use tokio::fs;

    use crate::log::config::Config;
    use crate::log::index::ENT_WIDTH;

    use super::{Record, Segment};

    #[tokio::test]
    async fn test_segment() {
        let tmp_dir = env::temp_dir();
        let tmp_dir_path = tmp_dir.join("segment-test");
        fs::create_dir(&tmp_dir_path).await.unwrap();

        let mut record = Record {
            offset: 0,
            value: b"hello world".to_vec(),
        };

        let mut config = Config::default();
        config.segment.max_store_bytes = 1024;
        config.segment.max_index_bytes = ENT_WIDTH * 3;

        let segment = Segment::new(&tmp_dir_path, 16, config).await.unwrap();

        assert_eq!(16, segment.next_offset().await);
        assert!(!segment.is_maxed().await);

        for i in 0..3 {
            let off = segment.append(&mut record).await.unwrap();
            assert_eq!(16 + i, off);

            let read_record = segment.read(off).await.unwrap();
            assert_eq!(read_record, record);
        }

        segment.append(&mut record).await.expect_err("expected unexpected eof error");
        assert!(segment.is_maxed().await);

        drop(segment);

        let mut config = Config::default();
        config.segment.max_store_bytes = record.value.len() as u64 * 3;
        config.segment.max_index_bytes = 1024;

        let segment = Segment::new(&tmp_dir_path, 16, config.clone()).await.unwrap();
        assert!(segment.is_maxed().await);

        fs::remove_file(tmp_dir_path.join("16.store")).await.unwrap();
        fs::remove_file(tmp_dir_path.join("16.index")).await.unwrap();
        let segment = Segment::new(&tmp_dir_path, 16, config).await.unwrap();
        assert!(!segment.is_maxed().await);
        fs::remove_dir_all(tmp_dir_path).await.unwrap()
    }
}