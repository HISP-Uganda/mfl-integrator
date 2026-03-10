package config

import "testing"

func TestDefaultPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		goos       string
		wantFile   string
		wantDir    string
		wantConfD  string
		expectFail bool
	}{
		{
			name:      "linux",
			goos:      "linux",
			wantFile:  "/etc/mflintegrator/mfld.yml",
			wantDir:   "/etc/mflintegrator/",
			wantConfD: "/etc/mflintegrator/conf.d",
		},
		{
			name:      "darwin",
			goos:      "darwin",
			wantFile:  "/etc/mflintegrator/mfld.yml",
			wantDir:   "/etc/mflintegrator/",
			wantConfD: "/etc/mflintegrator/conf.d",
		},
		{
			name:      "windows",
			goos:      "windows",
			wantFile:  "C:\\ProgramData\\MFLIntegrator\\mfld.yml",
			wantDir:   "C:\\ProgramData\\MFLIntegrator",
			wantConfD: "C:\\ProgramData\\MFLIntegrator\\conf.d",
		},
		{
			name:       "unsupported",
			goos:       "plan9",
			expectFail: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotFile, gotDir, gotConfD, err := defaultPaths(tc.goos)
			if tc.expectFail {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotFile != tc.wantFile || gotDir != tc.wantDir || gotConfD != tc.wantConfD {
				t.Fatalf("got (%q, %q, %q), want (%q, %q, %q)", gotFile, gotDir, gotConfD, tc.wantFile, tc.wantDir, tc.wantConfD)
			}
		})
	}
}
