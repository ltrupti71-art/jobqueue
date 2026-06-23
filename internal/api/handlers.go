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
		h.logger.Warn("submit: invalid JSON", "error", err)
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Type == "" {
		h.logger.Warn("submit: missing type")
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}

	job, err := h.svc.Submit(r.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidJobType):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, service.ErrInvalidTimeout):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, service.ErrInvalidRunAt):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, service.ErrInvalidDelay):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, service.ErrConflictingSchedule):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			h.logger.Error("submit job failed", "type", req.Type, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to submit job")
		}
		return
	}

	h.logger.Info("submit accepted", "job_id", job.ID, "type", job.Type)
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
		h.logger.Error("get job failed", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}

	writeJSON(w, http.StatusOK, job.ToResponse())
}

func (h *Handler) QueueDepth(w http.ResponseWriter, r *http.Request) {
	depth, err := h.svc.QueueDepth(r.Context())
	if err != nil {
		h.logger.Error("queue depth failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get queue depth")
		return
	}
	writeJSON(w, http.StatusOK, depth)
}

func (h *Handler) DrainQueue(w http.ResponseWriter, r *http.Request) {
	result, err := h.svc.Drain(r.Context())
	if err != nil {
		h.logger.Error("drain queue failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to drain queue")
		return
	}
	h.logger.Info("queue drain completed", "cancelled", result.Cancelled)
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
		switch {
		case errors.Is(err, service.ErrJobNotFound):
			writeError(w, http.StatusNotFound, "job not found")
		case errors.Is(err, service.ErrJobNotCancellable):
			writeError(w, http.StatusConflict, "job cannot be cancelled in current state")
		default:
			h.logger.Error("cancel job failed", "job_id", id, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to cancel job")
		}
		return
	}

	h.logger.Info("job cancelled via API", "job_id", id)
	writeJSON(w, http.StatusOK, job.ToResponse())
}

func (h *Handler) ListDeadLettered(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.svc.ListDeadLettered(r.Context())
	if err != nil {
		h.logger.Error("list dead-lettered failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list dead-lettered jobs")
		return
	}
	resp := make([]domain.JobResponse, 0, len(jobs))
	for _, job := range jobs {
		resp = append(resp, job.ToResponse())
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) CreateSchedule(w http.ResponseWriter, r *http.Request) {
	var req domain.CreateScheduleRequest
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

	sch, err := h.svc.CreateSchedule(r.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidJobType):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, service.ErrInvalidCron):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, service.ErrInvalidTimezone):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, service.ErrInvalidTimeout):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			h.logger.Error("create schedule failed", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to create schedule")
		}
		return
	}
	writeJSON(w, http.StatusCreated, sch.ToResponse())
}

func (h *Handler) GetSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "schedule id is required")
		return
	}
	sch, err := h.svc.GetSchedule(r.Context(), id)
	if err != nil {
		if errors.Is(err, service.ErrScheduleNotFound) {
			writeError(w, http.StatusNotFound, "schedule not found")
			return
		}
		h.logger.Error("get schedule failed", "schedule_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get schedule")
		return
	}
	writeJSON(w, http.StatusOK, sch.ToResponse())
}

func (h *Handler) ListSchedules(w http.ResponseWriter, r *http.Request) {
	schedules, err := h.svc.ListSchedules(r.Context())
	if err != nil {
		h.logger.Error("list schedules failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list schedules")
		return
	}
	resp := make([]domain.ScheduleResponse, 0, len(schedules))
	for _, sch := range schedules {
		resp = append(resp, sch.ToResponse())
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) CancelSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "schedule id is required")
		return
	}
	sch, err := h.svc.CancelSchedule(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrScheduleNotFound):
			writeError(w, http.StatusNotFound, "schedule not found")
		case errors.Is(err, service.ErrScheduleNotCancellable):
			writeError(w, http.StatusConflict, "schedule cannot be cancelled in current state")
		default:
			h.logger.Error("cancel schedule failed", "schedule_id", id, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to cancel schedule")
		}
		return
	}
	writeJSON(w, http.StatusOK, sch.ToResponse())
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write JSON response failed", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, domain.ErrorResponse{Error: msg})
}
