package s3

import (
	"errors"
	"strings"

	"github.com/TzimmesTeam/control-plane/internal/domain/models"
)

const (
	uriScheme = "s3://"

	inputDir    = "input"
	runsDir     = "runs"
	batchesDir  = "batches"
	preparedDir = "prepared"
	cashflowDir = "cashflow"
	pvDir       = "pv"

	dealsFile    = "deals.csv"
	curvesDir    = "curves"
	contextFile  = "context.json"
	manifestFile = "batches.json"
	idsExt       = ".ids"
)

var ErrInvalidURI = errors.New("invalid s3 uri")

func DealsKey(runID string) string {
	return inputDir + "/" + runID + "/" + dealsFile
}

func CurvesPrefix(runID string) string {
	return inputDir + "/" + runID + "/" + curvesDir + "/"
}

func ContextKey(runID string) string {
	return runsDir + "/" + runID + "/" + contextFile
}

func IdsKey(runID, batchID string) string {
	return runsDir + "/" + runID + "/" + batchesDir + "/" + batchID + idsExt
}

func BatchesKey(runID string) string {
	return runsDir + "/" + runID + "/" + manifestFile
}

func StageRunPrefix(stage models.Stage, runID string) string {
	return stageDir(stage) + "/" + runID + "/"
}

func StagePrefix(stage models.Stage, runID, batchID string) string {
	return StageRunPrefix(stage, runID) + batchID + "/"
}

func ReportPrefix(runID string) string {
	return runID + "/"
}

func URI(bucket, key string) string {
	return uriScheme + bucket + "/" + key
}

func ParseURI(uri string) (bucket, key string, err error) {
	rest, ok := strings.CutPrefix(uri, uriScheme)
	if !ok {
		return "", "", ErrInvalidURI
	}

	bucket, key, ok = strings.Cut(rest, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", ErrInvalidURI
	}

	return bucket, key, nil
}

func stageDir(stage models.Stage) string {
	switch stage {
	case models.StagePrep:
		return preparedDir
	case models.StageCashflow:
		return cashflowDir
	case models.StagePV:
		return pvDir
	default:
		return string(stage)
	}
}
