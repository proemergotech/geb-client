package geb

import (
	"testing"

	"github.com/proemergotech/errors"
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
		data := data
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

func TestRetryMiddleware(t *testing.T) {
	var iteration = 0

	testCases := map[string]struct {
		next             func(*Event) error
		wantedIterations int
	}{
		"error": {
			next: func(*Event) error {
				if iteration >= 5 {
					return nil
				}
				iteration++
				return errors.New("test")
			},
			wantedIterations: 5,
		},
		"happy": {
			next: func(*Event) error {
				iteration++
				return nil
			},
			wantedIterations: 1,
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			iteration = 0

			m := RetryMiddleware()
			_ = m(&Event{}, testCase.next)

			if iteration < testCase.wantedIterations {
				t.Errorf("want: 5 runs, got: %d runs", iteration)
			}
		})
	}
}
