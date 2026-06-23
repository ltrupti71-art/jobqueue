package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/jobqueue/api/internal/domain"
	"github.com/jobqueue/api/internal/service"
)

type Handler struct {
	svc    *service.Service
	logger *slog.Logger
}

func NewHandler(svc *service.Service, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{svc: svc, logger: logger}
}

func (h *Handler) SubmitJob(w http.ResponseWriter, r *http.Request) {
	var req domain.SubmitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}

	job, err := h.svc.Submit(r.Context(), req)
	if err != nil {
		if errors.Is(err, service.ErrInvalidJobType) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		h.logger.Error("submit job failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to submit job")
		return
	}

	writeJSON(w, http.StatusAccepted, job.ToResponse())
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job id is required")
		return
	}

	job, err := h.svc.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, service.ErrJobNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}

	writeJSON(w, http.StatusOK, job.ToResponse())
}

func (h *Handler) QueueDepth(w http.ResponseWriter, r *http.Request) {
	depth, err := h.svc.QueueDepth(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get queue depth")
		return
	}
	writeJSON(w, http.StatusOK, depth)
}

func (h *Handler) DrainQueue(w http.ResponseWriter, r *http.Request) {
	result, err := h.svc.Drain(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to drain queue")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job id is required")
		return
	}

	job, err := h.svc.Cancel(r.Context(), id)
	if err != nil {
		if errors.Is(err, service.ErrJobNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		if errors.Is(err, service.ErrJobNotCancellable) {
			writeError(w, http.StatusConflict, "job cannot be cancelled in current state")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
		return
	}

	writeJSON(w, http.StatusOK, job.ToResponse())
}

func (h *Handler) ListDeadLettered(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.svc.ListDeadLettered(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list dead-lettered jobs")
		return
	}
	resp := make([]domain.JobResponse, 0, len(jobs))
	for _, job := range jobs {
		resp = append(resp, job.ToResponse())
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, domain.ErrorResponse{Error: msg})
}
