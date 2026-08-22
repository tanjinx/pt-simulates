package repllag

import "testing"

func TestStatusHealthy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		st   Status
		want bool
	}{
		{
			name: "ok",
			st: Status{
				IORunning:  "Yes",
				SQLRunning: "Yes",
				Fields:     map[string]string{},
			},
			want: true,
		},
		{
			name: "io stopped",
			st: Status{
				IORunning:  "No",
				SQLRunning: "Yes",
				Fields:     map[string]string{},
			},
			want: false,
		},
		{
			name: "sql error",
			st: Status{
				IORunning:  "Yes",
				SQLRunning: "Yes",
				Fields:     map[string]string{"Last_SQL_Error": "duplicate key"},
			},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.st.Healthy(); got != tc.want {
				t.Fatalf("Healthy() = %v, want %v", got, tc.want)
			}
		})
	}
}
