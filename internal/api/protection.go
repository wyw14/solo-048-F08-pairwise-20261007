package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"heatpulse/internal/protection"
)

func (a *App) startAntifreeze(w http.ResponseWriter, r *http.Request) {
	session, err := a.Antifreeze.Start(r.Context(), chi.URLParam(r, "stationID"))
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, session)
}

func (a *App) stopAntifreeze(w http.ResponseWriter, r *http.Request) {
	session, err := a.Antifreeze.Stop(chi.URLParam(r, "stationID"))
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (a *App) getAntifreeze(w http.ResponseWriter, r *http.Request) {
	session, err := a.Antifreeze.Session(chi.URLParam(r, "stationID"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (a *App) listAntifreeze(w http.ResponseWriter, _ *http.Request) {
	sessions, err := a.Antifreeze.Sessions()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sessions": sessions,
		"recovery": a.AntifreezeRecovery,
	})
}

func (a *App) resumeAntifreeze(w http.ResponseWriter, r *http.Request) {
	session, err := a.Antifreeze.Resume(r.Context(), chi.URLParam(r, "stationID"))
	if err != nil {
		if errors.Is(err, protection.ErrNoSession) {
			writeError(w, http.StatusNotFound, err)
		} else {
			writeError(w, http.StatusConflict, err)
		}
		return
	}
	writeJSON(w, http.StatusAccepted, session)
}

func (a *App) abandonAntifreeze(w http.ResponseWriter, r *http.Request) {
	session, err := a.Antifreeze.Abandon(chi.URLParam(r, "stationID"))
	if err != nil {
		if errors.Is(err, protection.ErrNoSession) {
			writeError(w, http.StatusNotFound, err)
		} else {
			writeError(w, http.StatusConflict, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (a *App) listIncidents(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.Protection.List())
}

func (a *App) resolveIncident(w http.ResponseWriter, r *http.Request) {
	incident, err := a.Protection.Resolve(chi.URLParam(r, "incidentID"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	a.LeakProtection.Clear(incident.StationID)
	writeJSON(w, http.StatusOK, incident)
}

func (a *App) applyLatch(w http.ResponseWriter, r *http.Request) {
	var input protection.Latches
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	input.StationID = chi.URLParam(r, "stationID")
	result, err := a.Latches.Apply(input)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *App) resetLatches(w http.ResponseWriter, r *http.Request) {
	stationID := chi.URLParam(r, "stationID")
	current := a.Latches.Get(stationID)
	result, err := a.Latches.Reset(stationID, current.Revision+1)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *App) enterMaintenance(w http.ResponseWriter, r *http.Request) {
	session, err := a.Maintenance.Enter(chi.URLParam(r, "stationID"))
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, session)
}

func (a *App) exitMaintenance(w http.ResponseWriter, r *http.Request) {
	result, err := a.Maintenance.Exit(chi.URLParam(r, "stationID"))
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
