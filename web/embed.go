package web

import "embed"

//go:embed index.html app.js style.css
var files embed.FS

func Files() embed.FS { return files }
