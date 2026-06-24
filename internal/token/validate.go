// Package token provides utilities for working with landscape tokens.
package token

import (
	"fmt"
	"sync"
)

// A landscape token is the identifier of a visualization landscape within ExplorViz.
// Tokens can be created or deleted by the user within the frontend.
// The token's secret value protects unauthorized users from writing to a landscape.
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
// Should not be initialized directly; use [NewInMemTokenStore] instead.
type InMemTokenStore struct {
	mu sync.RWMutex
	m  map[string]string
}

// NewInMemTokenStore initializes a new, empty [InMemTokenStore] along with its internal map.
func NewInMemTokenStore() *InMemTokenStore {
	return &InMemTokenStore{
		m: make(map[string]string),
	}
}

// Validate checks whether the provided token currently exists in the store and returns an error if it does not.
func (s *InMemTokenStore) Validate(t LandscapeToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.m[t.ID]
	if ok && val == t.Secret {
		return nil
	}
	return fmt.Errorf("unknown landscape token or incorrect secret")
}

// Put writes the provided landscape token to the store.
func (s *InMemTokenStore) Put(t LandscapeToken) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[t.ID] = t.Secret
}

// Delete removes the provided landscape token from the store.
func (s *InMemTokenStore) Delete(tokenID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.m, tokenID)
}

var _ TokenValidator = (*InMemTokenStore)(nil)

// A NoOpTokenValidator stores no data and treats all landscape token as valid.
type NoOpTokenValidator struct{}

// Validate returns nil on all inputs.
func (s NoOpTokenValidator) Validate(t LandscapeToken) error {
	return nil
}

var _ TokenValidator = NoOpTokenValidator{}
