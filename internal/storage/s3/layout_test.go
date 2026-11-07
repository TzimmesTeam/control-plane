package s3

import (
	"testing"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
	"github.com/stretchr/testify/require"
)

const (
	testRunID   = "11111111-1111-4111-8111-111111111111"
	testBatchID = "22222222-2222-4222-8222-222222222222"
)

func TestLayoutKeys(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"deals", DealsKey(testRunID), "input/" + testRunID + "/deals.csv"},
		{"curves", CurvesPrefix(testRunID), "input/" + testRunID + "/curves/"},
		{"context", ContextKey(testRunID), "runs/" + testRunID + "/context.json"},
		{"ids", IdsKey(testRunID, testBatchID), "runs/" + testRunID + "/batches/" + testBatchID + ".ids"},
		{"manifest", BatchesKey(testRunID), "runs/" + testRunID + "/batches.json"},
		{"prep prefix", StagePrefix(models.StagePrep, testRunID, testBatchID), "prepared/" + testRunID + "/" + testBatchID + "/"},
		{"cashflow prefix", StagePrefix(models.StageCashflow, testRunID, testBatchID), "cashflow/" + testRunID + "/" + testBatchID + "/"},
		{"pv prefix", StagePrefix(models.StagePV, testRunID, testBatchID), "pv/" + testRunID + "/" + testBatchID + "/"},
		{"prep run prefix", StageRunPrefix(models.StagePrep, testRunID), "prepared/" + testRunID + "/"},
		{"report prefix", ReportPrefix(testRunID), testRunID + "/"},
		{"uri", URI("data-lake", DealsKey(testRunID)), "s3://data-lake/input/" + testRunID + "/deals.csv"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}
}

func TestParseURI(t *testing.T) {
	tests := []struct {
		name       string
		uri        string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{name: "object", uri: "s3://data-lake/input/deals.csv", wantBucket: "data-lake", wantKey: "input/deals.csv"},
		{name: "prefix", uri: "s3://data-lake/input/curves/", wantBucket: "data-lake", wantKey: "input/curves/"},
		{name: "no scheme", uri: "data-lake/input/deals.csv", wantErr: true},
		{name: "wrong scheme", uri: "http://data-lake/deals.csv", wantErr: true},
		{name: "bucket only", uri: "s3://data-lake", wantErr: true},
		{name: "empty key", uri: "s3://data-lake/", wantErr: true},
		{name: "empty bucket", uri: "s3:///deals.csv", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := ParseURI(tt.uri)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidURI)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantBucket, bucket)
			require.Equal(t, tt.wantKey, key)
		})
	}
}
