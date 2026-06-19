// Package token provides utilities for working with landscape tokens.
package token

import (
	"fmt"
	"sync"
)

type LandscapeToken struct {
	ID     string
	Secret string
}

// A TokenValidator validates a landscape token, identified by its ID and secret.
type TokenValidator interface {
	// Validate checks whether the given landscape token meets the requirements of this validator
	// and returns an error if this is not the case.
	Validate(token LandscapeToken) error
}

// An InMemTokenStore uses a map to keep track of landscape tokens in-memory.
// Tokens can be manually added and removed from the store.
// The store is safe to read and write from multiple goroutines.
type InMemTokenStore struct {
	mu sync.RWMutex
	m  map[string]string
}

func NewInMemTokenStore() *InMemTokenStore {
	return &InMemTokenStore{
		m: make(map[string]string),
	}
}

func (s *InMemTokenStore) Validate(t LandscapeToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.m[t.ID]
	if ok && val == t.Secret {
		return nil
	}
	return fmt.Errorf("unknown landscape token or incorrect secret")
}

func (s *InMemTokenStore) Put(t LandscapeToken) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[t.ID] = t.Secret
}

func (s *InMemTokenStore) Delete(tokenID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.m, tokenID)
}

var _ TokenValidator = (*InMemTokenStore)(nil)

// A NoOpTokenValidator stores no data and treats all landscape token as valid.
type NoOpTokenValidator struct{}

func (s NoOpTokenValidator) Validate(t LandscapeToken) error {
	return nil
}

var _ TokenValidator = NoOpTokenValidator{}
