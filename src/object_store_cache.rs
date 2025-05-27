use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::{Display, Formatter},
    ops::Range,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{lock::Mutex, stream::BoxStream};
use leptos::logging::log;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, path::Path,
};
use object_store_opendal::OpendalStore;

#[derive(Debug)]
pub(crate) struct ObjectStoreCache {
    inner: OpendalStore,
    cache: Mutex<HashMap<(Path, Range<u64>), Bytes>>,
}

impl ObjectStoreCache {
    pub(crate) fn new(inner: OpendalStore) -> Self {
        Self {
            inner,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

impl Display for ObjectStoreCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStoreCache")
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreCache {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult, object_store::Error> {
        self.inner.get(location).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta, object_store::Error> {
        self.inner.head(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        return self.inner.get_opts(location, options).await;
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<u64>,
    ) -> Result<Bytes, object_store::Error> {
        let key = (location.clone(), range);
        let mut cache = self.cache.lock().await;
        let bytes = match cache.entry(key) {
            Entry::Occupied(o) => {
                log!(
                    "Request hit cache, path {}, range: {:?}",
                    location,
                    o.key().1
                );
                o.get().clone()
            }
            Entry::Vacant(v) => {
                let k = v.key();
                let bs = self.inner.get_range(location, k.1.clone()).await?;
                v.insert(bs.clone());
                bs
            }
        };
        Ok(bytes)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let mut tasks = Vec::with_capacity(ranges.len());
        for range in ranges {
            let task = self.get_range(location, range.clone());
            tasks.push(task);
        }
        let results = futures::future::join_all(tasks).await;
        Ok(results.into_iter().map(|r| r.unwrap()).collect())
    }

    async fn delete(&self, location: &Path) -> Result<(), object_store::Error> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), object_store::Error> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<(), object_store::Error> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
