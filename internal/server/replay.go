package server

import (
	"errors"

	"quorum/internal/statemachine"
	"quorum/internal/storage"
)

func ReplayCommittedLog(dir string, machine *statemachine.KV) error {
	state, err := storage.LoadRaftState(dir)
	if err != nil {
		return err
	}
	for _, ent := range state.Entries {
		if ent.Index == 0 || ent.Index > state.HardState.Commit || len(ent.Data) == 0 {
			continue
		}
		cmd, err := decodeCommand(ent.Data)
		if err != nil {
			return err
		}
		cmd.LogIndex = ent.Index
		if _, err := machine.Apply(cmd); err != nil && !isReplayNoopError(err) {
			return err
		}
	}
	return nil
}

func isReplayNoopError(err error) bool {
	return errors.Is(err, statemachine.ErrKeyNotFound) ||
		errors.Is(err, statemachine.ErrCompareFailed) ||
		errors.Is(err, statemachine.ErrDuplicateLower)
}
