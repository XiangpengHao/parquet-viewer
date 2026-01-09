use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    ops::Range,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{lock::Mutex, stream::BoxStream};
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
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
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        unimplemented!()
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
        self.get_ranges(location, &[range])
            .await
            .map(|mut v| v.remove(0))
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        // Check cache for all ranges
        let cache = self.cache.lock().await;
        let mut missing_ranges = Vec::new();
        let mut results = Vec::with_capacity(ranges.len());

        for range in ranges {
            let key = (location.clone(), range.clone());
            if let Some(bytes) = cache.get(&key) {
                tracing::info!("Request hit cache, path {}, range: {:?}", location, range);
                results.push(Some(bytes.clone()));
            } else {
                results.push(None);
                missing_ranges.push(range.clone());
            }
        }

        // Release lock before making network requests
        drop(cache);

        // Fetch all missing ranges in parallel
        if !missing_ranges.is_empty() {
            let fetch_tasks: Vec<_> = missing_ranges
                .iter()
                .map(|range| self.inner.get_range(location, range.clone()))
                .collect();

            let fetched = futures::future::join_all(fetch_tasks).await;

            // Update cache with fetched results
            let mut cache = self.cache.lock().await;
            for (range, fetch_result) in missing_ranges.iter().zip(fetched.into_iter()) {
                let bytes = fetch_result?;
                let key = (location.clone(), range.clone());
                cache.insert(key, bytes.clone());

                // Fill in the results
                for (i, r) in ranges.iter().enumerate() {
                    if r == range && results[i].is_none() {
                        results[i] = Some(bytes.clone());
                        break;
                    }
                }
            }
        }

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
