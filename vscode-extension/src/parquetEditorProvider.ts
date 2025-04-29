import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs/promises';
import * as http from 'http';
import * as fsSync from 'fs';
import { AddressInfo } from 'net';

/**
 * Provider for Parquet Viewers.
 */
export class ParquetEditorProvider implements vscode.CustomReadonlyEditorProvider {
	public static readonly viewType = 'parquetViewer.parquet';

	// Fixed port for webview to access server
	private static readonly LOCAL_STATIC_PORT = 3000;

	private readonly _webviews = new Map<string, {
		readonly webview: vscode.WebviewPanel;
		readonly uri: vscode.Uri;
	}>();

	private readonly _servers = new Map<string, {
		server: http.Server;
		port: number;
		url: string;
		filename: string;
	}>();

	constructor(
		private readonly context: vscode.ExtensionContext
	) { }

	/**
	 * Called when our custom editor is opened.
	 */
	public async openCustomDocument(
		uri: vscode.Uri,
		openContext: vscode.CustomDocumentOpenContext,
		token: vscode.CancellationToken
	): Promise<vscode.CustomDocument> {
		// Create and return a simple document for this file
		return {
			uri, dispose: () => {
				// Clean up any file servers when the document is disposed
				const key = uri.toString();
				if (this._servers.has(key)) {
					this._servers.get(key)?.server.close();
					this._servers.delete(key);
				}
			}
		};
	}

	/**
	 * Called when our custom editor is resolved.
	 */
	public async resolveCustomEditor(
		document: vscode.CustomDocument,
		webviewPanel: vscode.WebviewPanel,
		token: vscode.CancellationToken
	): Promise<void> {
		// Add the webview to our internal map
		const key = document.uri.toString();
		this._webviews.set(key, {
			webview: webviewPanel,
			uri: document.uri
		});

		// Start the file server immediately to get the port
		const serverInfo = await this._startFileServer(document.uri);

		if (!serverInfo) {
			// Handle server startup failure
			webviewPanel.webview.html = `<html><body><h1>Error: Could not start parquet server</h1></body></html>`;
			return;
		}

		// Setup webview options with port mapping
		webviewPanel.webview.options = {
			enableScripts: true,
			localResourceRoots: [
				vscode.Uri.joinPath(this.context.extensionUri, 'dist')
			],
			portMapping: [
				{
					webviewPort: ParquetEditorProvider.LOCAL_STATIC_PORT,
					extensionHostPort: serverInfo.port
				}
			]
		};

		// Then set the HTML content
		webviewPanel.webview.html = await this._getHtmlForWebview(webviewPanel.webview);

		// Handle messages from the webview
		this._handleMessages(document, webviewPanel);

		// Clean up our resources when the webview is closed
		webviewPanel.onDidDispose(() => {
			this._webviews.delete(key);

			// Close the file server when the webview is closed
			if (this._servers.has(key)) {
				this._servers.get(key)?.server.close();
				this._servers.delete(key);
			}
		});
	}

	/**
	 * Get the HTML to show in the webview.
	 */
	private async _getHtmlForWebview(webview: vscode.Webview): Promise<string> {
		// Load the HTML content from dist/index.html
		const distPath = vscode.Uri.joinPath(this.context.extensionUri, 'dist');
		const indexPath = vscode.Uri.joinPath(distPath, 'index.html');

		try {
			// Read the file contents
			const htmlBytes = await vscode.workspace.fs.readFile(indexPath);
			let html = new TextDecoder().decode(htmlBytes);

			// Get all files in the dist directory
			const distFiles = await this._listFiles(distPath);

			// Replace all file references with webview URIs
			for (const file of distFiles) {
				// Skip index.html itself
				if (file === 'index.html') {
					continue;
				}

				// Create webview URI for this file
				const onDiskPath = vscode.Uri.joinPath(distPath, file);
				const webviewUri = webview.asWebviewUri(onDiskPath).toString();

				// Replace all occurrences of this file in the HTML
				// Replace both /file.js and file.js forms with quotes, parentheses, or in HTML attributes
				html = html.replace(new RegExp(`(['"\\(])/${file}(['"\\)])`, 'g'), `$1${webviewUri}$2`);
				html = html.replace(new RegExp(`(['"\\(])${file}(['"\\)])`, 'g'), `$1${webviewUri}$2`);
				// Additional replacements for unquoted attributes (like href=/file.js)
				html = html.replace(new RegExp(`(href=)/${file}(\\s|>)`, 'g'), `$1${webviewUri}$2`);
				html = html.replace(new RegExp(`(href=)${file}(\\s|>)`, 'g'), `$1${webviewUri}$2`);
				html = html.replace(new RegExp(`(src=)/${file}(\\s|>)`, 'g'), `$1${webviewUri}$2`);
				html = html.replace(new RegExp(`(src=)${file}(\\s|>)`, 'g'), `$1${webviewUri}$2`);
			}

			return html;
		} catch (error) {
			console.error('Failed to load HTML:', error);
			// Fallback to a basic error message
			return `<html><body><h1>Error: Could not load Parquet Viewer</h1><p>${error}</p></body></html>`;
		}
	}

	/**
	 * Recursively list all files in a directory
	 */
	private async _listFiles(dir: vscode.Uri, basePath: string = ''): Promise<string[]> {
		const files: string[] = [];

		try {
			const entries = await vscode.workspace.fs.readDirectory(dir);

			for (const [name, type] of entries) {
				const path = basePath ? `${basePath}/${name}` : name;

				if (type === vscode.FileType.File) {
					files.push(path);
				} else if (type === vscode.FileType.Directory) {
					const subFiles = await this._listFiles(vscode.Uri.joinPath(dir, name), path);
					files.push(...subFiles);
				}
			}
		} catch (error) {
			console.error(`Error listing files in ${dir}:`, error);
		}

		return files;
	}

	/**
	 * Handle messages from the webview
	 */
	private _handleMessages(document: vscode.CustomDocument, webviewPanel: vscode.WebviewPanel): void {
		webviewPanel.webview.onDidReceiveMessage(e => {
			switch (e.type) {
				case 'ready':
					// Send the server URL to the webview when it's ready
					const key = document.uri.toString();
					if (this._servers.has(key)) {
						const serverInfo = this._servers.get(key)!;
						// Ensure we don't have any double slashes in the URL
						const cleanFilename = serverInfo.filename.startsWith('/')
							? serverInfo.filename
							: `/${serverInfo.filename}`;
						webviewPanel.webview.postMessage({
							type: 'parquetServerReady',
							url: `http://localhost:${ParquetEditorProvider.LOCAL_STATIC_PORT}${cleanFilename}`,
							filename: serverInfo.filename
						});
					}
					break;
			}
		});
	}

	/**
	 * Start an HTTP server to serve the parquet file
	 */
	private async _startFileServer(uri: vscode.Uri): Promise<{ server: http.Server; port: number; url: string; filename: string } | undefined> {
		const key = uri.toString();
		const filename = path.basename(uri.fsPath);

		// Close any existing server for this file
		if (this._servers.has(key)) {
			this._servers.get(key)?.server.close();
			this._servers.delete(key);
		}

		try {
			// Create a simple HTTP server to serve the file
			const server = http.createServer((req, res) => {
				// Set CORS headers to allow access from the webview
				res.setHeader('Access-Control-Allow-Origin', '*');
				res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
				res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Range');
				res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');

				// Handle preflight requests
				if (req.method === 'OPTIONS') {
					res.writeHead(200);
					res.end();
					return;
				}

				// Get the requested path and normalize it to remove double slashes
				const reqPath = (req.url || '/').replace(/\/+/g, '/');

				// Serve the file if the path matches the parquet filename 
				// or is the root with the filename appended
				// Handle paths with or without leading slash, or with double slashes
				if ((req.method === 'GET' || req.method === 'HEAD') && (
					reqPath === `/${filename}` ||
					reqPath === filename ||
					reqPath === '/' ||
					// Match also paths with double slashes
					req.url === `//${filename}` ||
					req.url === `/${filename}`
				)) {
					// Get file stats to set proper headers
					const stats = fsSync.statSync(uri.fsPath);

					// Set basic headers
					res.setHeader('Content-Type', 'application/octet-stream');
					res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
					res.setHeader('Accept-Ranges', 'bytes');
					res.setHeader('Content-Length', stats.size);

					// For HEAD requests, just send headers
					if (req.method === 'HEAD') {
						res.statusCode = 200;
						res.end();
						return;
					}

					// Create file stream - Node.js handles range requests automatically
					const fileStream = fsSync.createReadStream(uri.fsPath, {
						// If Range header exists, createReadStream will handle it
						start: req.headers.range ? undefined : 0,
					});

					// Handle range requests
					if (req.headers.range) {
						const ranges = req.headers.range.replace(/bytes=/, '').split('-');
						const start = parseInt(ranges[0], 10);
						const end = ranges[1] ? parseInt(ranges[1], 10) : stats.size - 1;

						// Validate range
						if (isNaN(start) || isNaN(end) || start >= stats.size || end >= stats.size || start > end) {
							res.statusCode = 416; // Range Not Satisfiable
							res.setHeader('Content-Range', `bytes */${stats.size}`);
							res.end();
							return;
						}

						// Set range response headers
						res.statusCode = 206; // Partial Content
						res.setHeader('Content-Range', `bytes ${start}-${end}/${stats.size}`);
						res.setHeader('Content-Length', end - start + 1);

						// Create stream with range
						const rangeStream = fsSync.createReadStream(uri.fsPath, { start, end });
						rangeStream.pipe(res);
					} else {
						// Stream entire file
						fileStream.pipe(res);
					}

					// Handle errors
					fileStream.on('error', (error) => {
						console.error('Error streaming file:', error);
						res.statusCode = 500;
						res.end('Error reading file');
					});
				} else {
					console.log('Not found:', req.url, 'normalized:', reqPath);
					res.statusCode = 404;
					res.end('Not found');
				}
			});

			// Create a Promise to wait for the server to start
			return new Promise((resolve, reject) => {
				// Start the server on a random available port
				server.listen(0, 'localhost', () => {
					const address = server.address() as AddressInfo;
					const port = address.port;
					// Store simple filename without any slashes for consistency
					const cleanFilename = filename.replace(/^\/+/, '');
					// Create URL with the parquet filename in it
					const serverUrl = `http://localhost:${port}/${cleanFilename}`;
					console.log(`Parquet file server started on ${serverUrl}`);

					// Store the server info
					const serverInfo = {
						server: server,
						port: port,
						url: serverUrl,
						filename: cleanFilename // Store filename without leading slashes
					};

					this._servers.set(key, serverInfo);

					// Return the server info
					resolve(serverInfo);
				});

				// Add event listener for server errors
				server.on('error', (error) => {
					console.error('File server error:', error);
					reject(error);
				});
			});
		} catch (error) {
			console.error('Error starting file server:', error);
			return undefined;
		}
	}
}
