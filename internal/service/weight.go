package service

import (
	"strconv"
	"strings"
	"time"
)

const (
	defaultWeight int64 = 12
	minWeight     int64 = 1
	maxWeight     int64 = 2000

	periodEndDate = "ENDDATE"
	dealDateFmt   = "02.01.2006"

	daysPerDay   = 1
	daysPerMonth = 30
	daysPerYear  = 365
	hoursPerDay  = 24
)

func estimateWeight(regularPeriod, interestPeriod, reportDttm, maturityDttm string) int64 {
	period := strings.TrimSpace(regularPeriod)
	if period == "" {
		period = strings.TrimSpace(interestPeriod)
	}
	if period == "" || strings.EqualFold(period, periodEndDate) {
		return minWeight
	}

	periodDays, ok := parsePeriodDays(period)
	if !ok {
		return defaultWeight
	}

	from, err := time.Parse(dealDateFmt, strings.TrimSpace(reportDttm))
	if err != nil {
		return defaultWeight
	}
	to, err := time.Parse(dealDateFmt, strings.TrimSpace(maturityDttm))
	if err != nil {
		return defaultWeight
	}

	if !to.After(from) {
		return minWeight
	}

	durationDays := int64(to.Sub(from).Hours() / hoursPerDay)

	return clampWeight(durationDays / periodDays)
}

func parsePeriodDays(period string) (int64, bool) {
	if len(period) < 2 {
		return 0, false
	}

	unit := period[len(period)-1]
	n, err := strconv.ParseInt(period[:len(period)-1], 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}

	switch unit {
	case 'D', 'd':
		return n * daysPerDay, true
	case 'M', 'm':
		return n * daysPerMonth, true
	case 'Y', 'y':
		return n * daysPerYear, true
	default:
		return 0, false
	}
}

func clampWeight(w int64) int64 {
	return max(minWeight, min(w, maxWeight))
}
