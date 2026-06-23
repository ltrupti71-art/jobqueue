package api

import (
	"log/slog"
	"net/http"
)

func NewRouter(h *Handler, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("POST /jobs", h.SubmitJob)
	mux.HandleFunc("GET /jobs/{id}", h.GetJob)
	mux.HandleFunc("POST /jobs/{id}/cancel", h.CancelJob)
	mux.HandleFunc("GET /queue/depth", h.QueueDepth)
	mux.HandleFunc("POST /queue/drain", h.DrainQueue)
	mux.HandleFunc("GET /dead-letter", h.ListDeadLettered)

	var handler http.Handler = mux
	handler = Logging(logger)(handler)
	handler = Recovery(logger)(handler)
	return handler
}
