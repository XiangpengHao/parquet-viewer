import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs/promises';

/**
 * Provider for Parquet Viewers.
 */
export class ParquetEditorProvider implements vscode.CustomReadonlyEditorProvider {
	public static readonly viewType = 'parquetViewer.parquet';

	private readonly _webviews = new Map<string, {
		readonly webview: vscode.WebviewPanel;
		readonly uri: vscode.Uri;
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
		return { uri, dispose: () => { } };
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

		// Setup initial content for the webview
		webviewPanel.webview.options = {
			enableScripts: true,
			localResourceRoots: [
				vscode.Uri.joinPath(this.context.extensionUri, 'dist')
			]
		};

		webviewPanel.webview.html = await this._getHtmlForWebview(webviewPanel.webview);

		// Handle messages from the webview
		webviewPanel.webview.onDidReceiveMessage(e => {
			switch (e.type) {
				case 'ready':
					this._loadParquetFile(document.uri, webviewPanel);
					break;
			}
		});

		// Clean up our resources when the webview is closed
		webviewPanel.onDidDispose(() => {
			this._webviews.delete(key);
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
	 * Load and parse the parquet file, then send data to the webview
	 */
	private async _loadParquetFile(uri: vscode.Uri, webviewPanel: vscode.WebviewPanel): Promise<void> {
		try {
			// Read the file from the file system
			const fileData = await vscode.workspace.fs.readFile(uri);

			// Convert the Uint8Array to an array buffer to ensure proper serialization
			// when sending to the webview
			const arrayBuffer = fileData.buffer.slice(
				fileData.byteOffset,
				fileData.byteOffset + fileData.byteLength
			);

			// Send the binary data to the webview
			console.log(`Sending parquet data to webview: ${path.basename(uri.fsPath)}, size: ${fileData.byteLength} bytes`);
			webviewPanel.webview.postMessage({
				type: 'parquetData',
				data: arrayBuffer,
				filename: path.basename(uri.fsPath)
			});
		} catch (error) {
			console.error(error);
			webviewPanel.webview.postMessage({
				type: 'error',
				message: 'Error loading parquet file: ' + (error instanceof Error ? error.message : String(error))
			});
		}
	}
}
