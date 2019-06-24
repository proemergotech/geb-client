package geb

import (
	"testing"

	"github.com/pkg/errors"
)

func TestRecoveryMiddleware(t *testing.T) {
	for name, data := range map[string]struct {
		next    func(*Event) error
		wantErr error
	}{
		"panic": {
			next: func(*Event) error {
				panic(errors.New("test"))
			},
			wantErr: errors.New("panic recovered in geb recovery middleware: test"),
		},
		"error": {
			next: func(*Event) error {
				return errors.New("test")
			},
			wantErr: errors.New("test"),
		},
		"happy": {
			next: func(*Event) error {
				return nil
			},
			wantErr: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := RecoveryMiddleware()

			gotErr := m(&Event{}, data.next)

			want := "nil"
			if data.wantErr != nil {
				want = data.wantErr.Error()
			}
			got := "nil"
			if gotErr != nil {
				got = gotErr.Error()
			}

			if want != got {
				t.Errorf("want error: %v, got error: %v", want, got)
			}
		})
	}
}