package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEstimateWeight(t *testing.T) {
	tests := []struct {
		name           string
		regularPeriod  string
		interestPeriod string
		reportDttm     string
		maturityDttm   string
		want           int64
	}{
		{name: "1M for 30 years", regularPeriod: "1M", reportDttm: "31.08.2022", maturityDttm: "31.08.2052", want: 365},
		{name: "ENDDATE", regularPeriod: "ENDDATE", reportDttm: "31.08.2022", maturityDttm: "31.08.2052", want: 1},
		{name: "45D for one year", regularPeriod: "45D", reportDttm: "31.08.2022", maturityDttm: "31.08.2023", want: 8},
		{name: "1Y for 10 years", regularPeriod: "1Y", reportDttm: "31.08.2022", maturityDttm: "31.08.2032", want: 10},
		{name: "empty periods", reportDttm: "31.08.2022", maturityDttm: "31.08.2052", want: 1},
		{name: "fallback to interest period", interestPeriod: "3M", reportDttm: "31.08.2022", maturityDttm: "31.08.2023", want: 4},
		{name: "broken report date", regularPeriod: "1M", reportDttm: "2022-08-31", maturityDttm: "31.08.2052", want: defaultWeight},
		{name: "broken maturity date", regularPeriod: "1M", reportDttm: "31.08.2022", maturityDttm: "garbage", want: defaultWeight},
		{name: "broken period", regularPeriod: "monthly", reportDttm: "31.08.2022", maturityDttm: "31.08.2052", want: defaultWeight},
		{name: "zero period", regularPeriod: "0M", reportDttm: "31.08.2022", maturityDttm: "31.08.2052", want: defaultWeight},
		{name: "maturity before report", regularPeriod: "1M", reportDttm: "31.08.2022", maturityDttm: "31.08.2020", want: 1},
		{name: "maturity equals report", regularPeriod: "1M", reportDttm: "31.08.2022", maturityDttm: "31.08.2022", want: 1},
		{name: "short deal rounds up to min", regularPeriod: "1M", reportDttm: "31.08.2022", maturityDttm: "10.09.2022", want: 1},
		{name: "capped at max weight", regularPeriod: "1D", reportDttm: "31.08.2022", maturityDttm: "31.08.2052", want: maxWeight},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateWeight(tt.regularPeriod, tt.interestPeriod, tt.reportDttm, tt.maturityDttm)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParsePeriodDays(t *testing.T) {
	tests := []struct {
		period string
		want   int64
		ok     bool
	}{
		{"1M", 30, true},
		{"2M", 60, true},
		{"45D", 45, true},
		{"153D", 153, true},
		{"1Y", 365, true},
		{"M", 0, false},
		{"1W", 0, false},
		{"-1M", 0, false},
		{"", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.period, func(t *testing.T) {
			got, ok := parsePeriodDays(tt.period)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}
