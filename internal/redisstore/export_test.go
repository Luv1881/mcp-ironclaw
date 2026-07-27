package redisstore

func (s *Store) ApplyScriptKeys(userID, deviceID string) []string {
	return []string{
		s.stateKey(userID, deviceID),
		s.dedupeKey(userID, deviceID),
		s.devicesKey(userID),
		s.updatesChannel(userID, deviceID),
	}
}

func (s *Store) ResetScriptKeys(userID, deviceID string) []string {
	return []string{
		s.stateKey(userID, deviceID),
		s.updatesChannel(userID, deviceID),
	}
}

func (s *Store) LockScriptKeys(resource string) []string {
	return []string{s.lockKey(resource), s.fenceKey(resource)}
}
