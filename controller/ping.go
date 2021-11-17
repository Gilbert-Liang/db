package controller

import (
	"encoding/json"
	"net/http"
)

func Ping(w http.ResponseWriter, r *http.Request) {
	verbose := r.URL.Query().Get("verbose")

	if verbose != "" && verbose != "0" && verbose != "false" {
		writeHeader(w, http.StatusOK)
		b, _ := json.Marshal(map[string]string{"version": "0.0.1"})
		_, _ = w.Write(b)
	} else {
		writeHeader(w, http.StatusNoContent)
	}
}
