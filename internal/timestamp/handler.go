package timestamp

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"
)

type Handler struct {
	repo Repository
}

func NewHandler(r Repository) Handler {
	return Handler{
		repo: r,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v3/landscapes/{landscapeToken}/timestamps", h.getTimestamps)
}

func (h *Handler) getTimestamps(w http.ResponseWriter, r *http.Request) {
	newest, err := strconv.ParseUint(r.URL.Query().Get("newest"), 10, 64)
	if err != nil {
		newest = math.MaxUint64
	}

	oldest, err := strconv.ParseUint(r.URL.Query().Get("oldest"), 10, 64)
	if err != nil {
		oldest = 0
	}

	bucketSize, err := strconv.ParseUint(r.URL.Query().Get("size"), 10, 64)
	if err != nil {
		duration := time.Duration(10) * time.Second
		bucketSize = uint64(duration.Nanoseconds())
	} else {
		bucketSize *= 1_000_000 // Milliseconds to nanoseconds
	}

	lt := r.PathValue("landscapeToken")
	if lt == "" {
		http.Error(w, "Missing or invalid landscape token in path parameter", http.StatusBadRequest)
		return
	}

	commit := r.URL.Query().Get("commit")

	ts, err := h.repo.findTimestamps(r.Context(), lt, oldest, newest, bucketSize, commit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(ts); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
