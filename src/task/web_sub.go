package task

import (
	"log"
	"net/http"
	"path/filepath"
)

// subPageHandler handles requests for /sub and /sub/sub.ts.
// Only these two paths are allowed; all others return 403 Forbidden.
type subPageHandler struct {
	staticDir string
}

// NewSubPageHandler creates a new subPageHandler.
// staticDir is the path to the static directory relative to the working directory.
func NewSubPageHandler(staticDir string) *subPageHandler {
	return &subPageHandler{staticDir: staticDir}
}

// allowedPaths lists the only URL paths this handler serves.
var allowedPaths = map[string]string{
	"/sub":        "index.html",
	"/sub/sub.ts": "sub.ts",
}

// ServeHTTP implements the http.Handler interface.
//
// Only the following paths are allowed:
//
//	GET /sub        -> serves index.html
//	GET /sub/sub.ts -> serves sub.ts
//
// All other paths return 403 Forbidden.
func (h *subPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	filename, ok := allowedPaths[r.URL.Path]
	if !ok {
		log.Printf("[sub-page] forbidden path: %s\n", r.URL.Path)
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	filePath := filepath.Join(h.staticDir, filename)
	log.Printf("[sub-page] serving: %s\n", filePath)
	http.ServeFile(w, r, filePath)
}
