package task

type HistorySyncer struct {
	timeStamp int64
}

func (h *HistorySyncer) Start(timeStamp int64) {
	h.timeStamp = timeStamp
}
