const fs = require('fs-extra');
const path = require('path');

async function copyDistFiles() {
	try {
		// Source directory (Dioxus build output)
		const sourceDir = path.resolve(__dirname, '../../target/dx/parquet-viewer/release/web/public');

		// Destination directory (VS Code extension dist)
		const destDir = path.resolve(__dirname, '../dist');

		// Remove existing destination directory if it exists
		if (await fs.pathExists(destDir)) {
			console.log(`Removing existing files in ${destDir}`);
			await fs.remove(destDir);
		}

		// Ensure destination directory exists
		await fs.ensureDir(destDir);

		// Copy files
		console.log(`Copying compiled assets from ${sourceDir} to ${destDir}`);
		await fs.copy(sourceDir, destDir);

		console.log('Successfully copied dist files');
	} catch (err) {
		console.error('Error copying dist files:', err);
		process.exit(1);
	}
}

copyDistFiles(); 