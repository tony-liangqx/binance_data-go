package task

type Subscriber struct {
	timeStamp int64
}

func (s *Subscriber) GetTimeStamp() int64 {
	return s.timeStamp
}

func (s *Subscriber) Start(timeStamp int64) {
	s.timeStamp = timeStamp
	s.start()
}

func (s *Subscriber) start() {
	// TODO: 消费websocket订阅的数据
}
