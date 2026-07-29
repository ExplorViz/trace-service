package trace

import (
	"encoding/json"
	"fmt"
	"net/http"
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
	mux.HandleFunc("DELETE /v3/landscapes/{landscapeToken}/trace-data", h.deleteTraceData)
}

func (h *Handler) deleteTraceData(w http.ResponseWriter, r *http.Request) {
	lt := r.PathValue("landscapeToken")
	if lt == "" {
		http.Error(w, "Missing or invalid landscape token in path parameter", http.StatusBadRequest)
		return
	}

	if err := h.repo.deleteAll(r.Context(), lt); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(fmt.Sprintf("Trace data successfully deleted for landscape %s", lt)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
