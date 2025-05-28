use std::{
    fmt::{Debug, Display, Formatter},
    ops::{Deref, DerefMut, Range},
    pin::Pin,
    task,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use futures::stream::BoxStream;
use leptos::logging;
use object_store::{
    Error as ObjectStoreError, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    path::Path,
};
use wasm_bindgen_futures::JsFuture;
use web_sys::js_sys::Uint8Array;

#[derive(Debug)]
pub struct WebFileObjectStore {
    inner: WebFileReader,
}

impl WebFileObjectStore {
    pub fn new(file: web_sys::File) -> Self {
        Self {
            inner: WebFileReader::new(file),
        }
    }
}

impl Display for WebFileObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WebFileObjectStore({})", self.inner.file_name())
    }
}

#[async_trait]
impl ObjectStore for WebFileObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        unreachable!()
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError> {
        unreachable!()
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        let meta = self.head(location).await?;
        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: 0..0,
                meta,
                attributes: Default::default(),
            });
        }

        let range = match options.range {
            Some(GetRange::Bounded(r)) => {
                if r.start >= r.end || r.start >= meta.size {
                    0..0
                } else {
                    let end = r.end.min(meta.size);
                    r.start..end
                }
            }
            Some(GetRange::Offset(r)) => {
                if r < meta.size {
                    r..meta.size
                } else {
                    0..0
                }
            }
            Some(GetRange::Suffix(r)) if r < meta.size => (meta.size - r)..meta.size,
            _ => 0..meta.size,
        };

        let web_file_reader_cloned = self.inner.clone();
        let range_for_stream = range.clone();

        let stream = futures::stream::once(async move {
            let get_range_future = web_file_reader_cloned.get_range(range_for_stream);

            let result_bytes = SendWrapper {
                inner: get_range_future,
            }
            .await;

            result_bytes.map_err(|e| ObjectStoreError::Generic {
                store: "WebFileObjectStore",
                source: anyhow::anyhow!(e).into(),
            })
        });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range,
            meta,
            attributes: Default::default(),
        })
    }

    async fn head(&self, _location: &Path) -> Result<ObjectMeta, ObjectStoreError> {
        Ok(self.inner.head())
    }

    async fn delete(&self, _location: &Path) -> Result<(), ObjectStoreError> {
        unreachable!()
    }

    fn list(
        &self,
        _prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        unreachable!()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> Result<ListResult, ObjectStoreError> {
        unreachable!()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<(), ObjectStoreError> {
        unreachable!()
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<(), ObjectStoreError> {
        unreachable!()
    }
}

#[derive(Debug, Clone)]
pub struct WebFileReader {
    file: web_sys::File,
    file_name: String,
}

unsafe impl Send for WebFileReader {}
unsafe impl Sync for WebFileReader {}

impl WebFileReader {
    pub fn new(file: web_sys::File) -> Self {
        let file_name = file.name();
        Self { file, file_name }
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    /// Get a slice of the file
    pub async fn get_range(&self, range: Range<u64>) -> Result<Bytes, String> {
        logging::log!("Fetching range {:?} from file", range);

        // Use the slice method to get only the requested range
        let blob = self
            .file
            .slice_with_i32_and_i32(range.start as i32, range.end as i32)
            .map_err(|e| format!("Failed to slice file: {e:?}"))?;

        let array_buffer = JsFuture::from(blob.array_buffer()).await.unwrap();

        // Convert to Uint8Array and then to a Rust Vec<u8>
        let uint8_array = Uint8Array::new(&array_buffer);
        let bytes = Bytes::from(uint8_array.to_vec());

        Ok(bytes)
    }

    pub fn head(&self) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(self.file_name.clone()),
            last_modified: DateTime::from_timestamp(self.file.last_modified() as i64, 0).unwrap(),
            size: self.file.size() as u64,
            e_tag: None,
            version: None,
        }
    }
}

struct SendWrapper<T> {
    inner: T,
}

unsafe impl<T> Send for SendWrapper<T> {}
unsafe impl<T> Sync for SendWrapper<T> {}

impl<T> Deref for SendWrapper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for SendWrapper<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T: Future> Future for SendWrapper<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        use std::ops::DerefMut;
        unsafe { self.map_unchecked_mut(Self::deref_mut) }.poll(cx)
    }
}
