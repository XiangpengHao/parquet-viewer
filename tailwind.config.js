/** @type {import('tailwindcss').Config} */
module.exports = {
	content: {
		files: ["*.html", "./src/**/*.rs"],
		transform: {
			rs: (content) => content.replace(/(?:^|\s)class:/g, ' '),
		},
	},
	theme: {
		extend: {
			colors: {
				primary: '#4a5568',
			},
			fontFamily: {
				sans: ['JetBrains Mono', 'Menlo', 'Monaco', 'Consolas', 'Liberation Mono', 'Courier New', 'monospace'],
			},
		},
	},
	plugins: [],
}