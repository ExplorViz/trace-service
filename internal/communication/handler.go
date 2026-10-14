package communication

import (
	"encoding/json"
	"net/http"
	"strconv"
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
	mux.HandleFunc("GET /v3/landscapes/{landscapeToken}/communication", h.getComm)
}

func (h *Handler) getComm(w http.ResponseWriter, r *http.Request) {
	lt := r.PathValue("landscapeToken")
	if lt == "" {
		http.Error(w, "Missing or invalid landscape token in path parameter", http.StatusBadRequest)
		return
	}

	query := r.URL.Query()

	searchParams := commSearchParams{
		FromUnixNano: parseUintOrNil(query.Get("from")),
		ToUnixNano:   parseUintOrNil(query.Get("to")),
		TraceID:      query.Get("traceId"),
		CommitHash:   query.Get("commit"),
	}

	cs, err := h.repo.findCommunication(r.Context(), lt, searchParams)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(cs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func parseUintOrNil(s string) *uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil
	}
	return &v
}
