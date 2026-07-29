package communication

import (
	"encoding/json"
	"math"
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
	mux.HandleFunc("GET /v3/landscapes/{landscapeToken}/communication/{sourceVizObjID}/{targetVizObjID}", h.getFuncs)
}

func (h *Handler) getComm(w http.ResponseWriter, r *http.Request) {
	lt := r.PathValue("landscapeToken")
	if lt == "" {
		http.Error(w, "Missing or invalid landscape token in path parameter", http.StatusBadRequest)
		return
	}

	from, err := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
	if err != nil {
		from = 0
	}

	to, err := strconv.ParseUint(r.URL.Query().Get("to"), 10, 64)
	if err != nil {
		to = math.MaxUint64
	}

	commit := r.URL.Query().Get("commit")

	cs, err := h.repo.findCommunication(r.Context(), lt, from, to, commit)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(cs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) getFuncs(w http.ResponseWriter, r *http.Request) {
	lt := r.PathValue("landscapeToken")
	if lt == "" {
		http.Error(w, "Missing or invalid landscape token in path parameter", http.StatusBadRequest)
		return
	}

	srcID := r.PathValue("sourceVizObjID")
	if srcID == "" {
		http.Error(w, "Missing or invalid source visualization object ID in path parameter", http.StatusBadRequest)
		return
	}

	tgtID := r.PathValue("targetVizObjID")
	if tgtID == "" {
		http.Error(w, "Missing or invalid target visualization object ID in path parameter", http.StatusBadRequest)
		return
	}

	from, err := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
	if err != nil {
		from = 0
	}

	to, err := strconv.ParseUint(r.URL.Query().Get("to"), 10, 64)
	if err != nil {
		to = math.MaxUint64
	}

	commit := r.URL.Query().Get("commit")

	fcs, err := h.repo.findFileCommDetails(r.Context(), lt, srcID, tgtID, from, to, commit)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(fcs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
