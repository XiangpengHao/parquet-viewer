import * as vscode from 'vscode';
import { ParquetEditorProvider } from './parquetEditorProvider';

export function activate(context: vscode.ExtensionContext) {
	// Register our custom editor provider
	const parquetEditorProvider = new ParquetEditorProvider(context);
	const providerRegistration = vscode.window.registerCustomEditorProvider(
		ParquetEditorProvider.viewType,
		parquetEditorProvider,
		{
			// Keep the webview alive even when not visible
			webviewOptions: {
				retainContextWhenHidden: true,

			},
			supportsMultipleEditorsPerDocument: false,
		},
	);

	context.subscriptions.push(providerRegistration);
}

export function deactivate() {
	// Clean up resources
} 