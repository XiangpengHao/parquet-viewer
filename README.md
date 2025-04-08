# Parquet Viewer

Online at: https://parquet-viewer.xiangpeng.systems

### Features

- Query parquet data with SQL ✅
- Query parquet data with natural language through LLM  ✅
- View Parquet metadata ✅
- View Parquet files from anywhere -- local file, S3, or any URLs ✅
- Everything runs in the browser, no server, no external dependencies, just a web page ✅
- Read only you query -- won't download the entire parquet file ✅

### Demo

![screenshot](doc/parquet-viewer.gif)

### Tips 

- You can use `?url=` to load a file from a url.
For example, [`parquet-viewer.xiangpeng.systems/?url=https://raw.githubusercontent.com/tobilg/public-cloud-provider-ip-ranges/main/data/providers/all.parquet`](https://parquet-viewer.xiangpeng.systems/?url=https://raw.githubusercontent.com/tobilg/public-cloud-provider-ip-ranges/main/data/providers/all.parquet) will load the file from github.
`parquet-viewer` is smart enough to only download the data that is relevant to your query, usually a few KBs, even if the file is large.

- You can use `parquet-viewer.py` in `utils` to open a local file. Only works on Chrome or Firefox (not Safari).
```bash
./parquet-viewer.py /path/to/your/file.parquet
```



## Development

It compiles [Parquet](https://github.com/apache/arrow-rs), [Arrow](https://github.com/apache/arrow-rs), [Datafusion](https://github.com/apache/datafusion), [OpenDAL](https://github.com/apache/opendal) to WebAssembly and uses it to explore Parquet files, [more details](https://blog.haoxp.xyz/posts/parquet-viewer/).


Checkout the awesome [Leptos](https://github.com/leptos-rs/leptos) framework.

#### Run locally
```bash
cargo install trunk --locked

trunk serve --release  --no-autoreload
```

#### Run tests

```bash
cargo install wasm-pack --locked
wasm-pack test --headless --firefox
```

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

Be aware that most of the code is generated by AI, resistance is futile.
