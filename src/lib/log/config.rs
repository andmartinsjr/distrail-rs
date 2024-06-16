#[derive(Clone, Debug, Default)]
pub struct SegmentConfig {
    pub max_store_bytes: u64,
    pub max_index_bytes: u64,
    pub initial_offset: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Config {
    pub segment: SegmentConfig,
}
