package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

const (
	testNamespace = namespace.Name("test-namespace")
)

func TestFieldSaAggInterceptor(t *testing.T) {
	s := require.New(t)
	saInterceptor := newSaAggInterceptor()
	intCol := query.NewSAColumn(
		"AliasForCustomIntField",
		"CustomIntField",
		enumspb.INDEXED_VALUE_TYPE_INT,
	)
	keywordCol := query.NewSAColumn(
		"AliasForCustomKeywordField",
		"CustomKeywordField",
		enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	)
	startTimeCol := query.NewSAColumn(
		sadefs.StartTime,
		sadefs.StartTime,
		enumspb.INDEXED_VALUE_TYPE_DATETIME,
	)

	err := saInterceptor.Intercept(intCol)
	s.NoError(err)
	s.Equal(map[string]struct{}{"AliasForCustomIntField": struct{}{}}, saInterceptor.names)

	err = saInterceptor.Intercept(keywordCol)
	s.NoError(err)
	s.Equal(
		map[string]struct{}{
			"AliasForCustomIntField":     struct{}{},
			"AliasForCustomKeywordField": struct{}{},
		},
		saInterceptor.names,
	)

	err = saInterceptor.Intercept(intCol)
	s.NoError(err)
	s.Equal(
		map[string]struct{}{
			"AliasForCustomIntField":     struct{}{},
			"AliasForCustomKeywordField": struct{}{},
		},
		saInterceptor.names,
	)

	err = saInterceptor.Intercept(startTimeCol)
	s.NoError(err)
	s.Equal(
		map[string]struct{}{
			"AliasForCustomIntField":     struct{}{},
			"AliasForCustomKeywordField": struct{}{},
			sadefs.StartTime:             struct{}{},
		},
		saInterceptor.names,
	)
}

func TestGetQueryFields(t *testing.T) {
	testCases := []struct {
		name           string
		input          string
		expectedFields []string
		expectedErrMsg string
	}{
		{
			name:           "empty query string",
			input:          "",
			expectedFields: []string{},
			expectedErrMsg: "",
		},
		{
			name:           "filter custom search attribute",
			input:          "AliasForKeyword01 = 'foo'",
			expectedFields: []string{"AliasForKeyword01"},
			expectedErrMsg: "",
		},
		{
			name:           "filter multiple custom search attribute",
			input:          "(AliasForKeyword01 = 'foo' AND AliasForInt01 = 123) OR AliasForKeyword01 = 'bar'",
			expectedFields: []string{"AliasForKeyword01", "AliasForInt01"},
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused",
			input:          "TemporalSchedulePaused = true",
			expectedFields: []string{"TemporalSchedulePaused"},
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused",
			input:          "TemporalSchedulePaused = true",
			expectedFields: []string{"TemporalSchedulePaused"},
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused and custom search attribute",
			input:          "TemporalSchedulePaused = true AND AliasForKeyword01 = 'foo'",
			expectedFields: []string{"TemporalSchedulePaused", "AliasForKeyword01"},
			expectedErrMsg: "",
		},
		{
			name:           "filter system search attribute",
			input:          "ExecutionDuration > '1s'",
			expectedFields: []string{"ExecutionDuration"},
			expectedErrMsg: "",
		},
		{
			name:           "invalid query filter",
			input:          "AliasForKeyword01 = foo",
			expectedFields: nil,
			expectedErrMsg: "invalid query",
		},
		{
			name:           "invalid custom search attribute",
			input:          "Foo = 'bar'",
			expectedFields: nil,
			expectedErrMsg: "invalid search attribute: Foo",
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				s := require.New(t)
				fields, err := getQueryFields(
					testNamespace,
					searchattribute.TestNameTypeMap(),
					searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{}),
					nil, // chasmMapper
					tc.input,
					metrics.NoopMetricsHandler,
					log.NewNoopLogger(),
				)
				if tc.expectedErrMsg == "" {
					s.NoError(err)
					s.Len(fields, len(tc.expectedFields))
					for _, f := range fields {
						s.Contains(tc.expectedFields, f)
					}
				} else {
					var invalidArgErr *serviceerror.InvalidArgument
					s.ErrorAs(err, &invalidArgErr)
					s.ErrorContains(err, tc.expectedErrMsg)
				}
			},
		)
	}
}

func TestValidateVisibilityQuery(t *testing.T) {
	testCases := []struct {
		name           string
		input          string
		expectedErrMsg string
	}{
		{
			name:           "empty query string",
			input:          "",
			expectedErrMsg: "",
		},
		{
			name:           "filter custom search attribute",
			input:          "AliasForKeyword01 = 'foo'",
			expectedErrMsg: "",
		},
		{
			name:           "filter multiple custom search attribute",
			input:          "(AliasForKeyword01 = 'foo' AND AliasForInt01 = 123) OR AliasForKeyword01 = 'bar'",
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused",
			input:          "TemporalSchedulePaused = true",
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused",
			input:          "TemporalSchedulePaused = true",
			expectedErrMsg: "",
		},
		{
			name:           "filter TemporalSchedulePaused and custom search attribute",
			input:          "TemporalSchedulePaused = true AND AliasForKeyword01 = 'foo'",
			expectedErrMsg: "",
		},
		{
			name:           "invalid filter system search attribute",
			input:          "ExecutionDuration > '1s'",
			expectedErrMsg: "invalid query filter for schedules: cannot filter on \"ExecutionDuration\"",
		},
		{
			name:           "invalid query filter",
			input:          "AliasForKeyword01 = foo",
			expectedErrMsg: "invalid query",
		},
		{
			name:           "invalid custom search attribute",
			input:          "Foo = foo",
			expectedErrMsg: "invalid search attribute: Foo",
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				s := require.New(t)
				err := ValidateVisibilityQuery(
					testNamespace,
					searchattribute.TestNameTypeMap(),
					searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{}),
					nil, // chasmMapper
					tc.input,
					metrics.NoopMetricsHandler,
					log.NewNoopLogger(),
				)
				if tc.expectedErrMsg == "" {
					s.NoError(err)
				} else {
					var invalidArgErr *serviceerror.InvalidArgument
					s.ErrorAs(err, &invalidArgErr)
					s.ErrorContains(err, tc.expectedErrMsg)
				}
			},
		)
	}
}

func TestValidateIsolatedVisibilityQuery(t *testing.T) {
	provider := searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{})
	validate := func(predicate string, isolated bool) error {
		if isolated {
			return ValidateIsolatedVisibilityQuery(testNamespace, searchattribute.TestNameTypeMap(), provider, nil, predicate, metrics.NoopMetricsHandler, log.NewNoopLogger())
		}
		return ValidateVisibilityQuery(testNamespace, searchattribute.TestNameTypeMap(), provider, nil, predicate, metrics.NoopMetricsHandler, log.NewNoopLogger())
	}
	for _, predicate := range []string{"NamespaceDivision = 'other'", "TemporalNamespaceDivision = 'other'", "NOT (NamespaceDivision = 'other')", "AliasForKeyword01 = 'foo' OR NamespaceDivision IN ('other')"} {
		t.Run(predicate, func(t *testing.T) {
			var invalid *serviceerror.InvalidArgument
			require.ErrorAs(t, validate(predicate, true), &invalid)
			require.ErrorContains(t, validate(predicate, true), "namespace division")
		})
	}
	// The stricter path is opt-in; existing callers retain their behavior.
	require.NoError(t, validate("NamespaceDivision = 'other'", false))
	for _, predicate := range []string{"", "AliasForKeyword01 = 'custom-value'", "AliasForKeyword01 IN ('a', 'b')", "AliasForKeyword01 != 'id'", "TemporalSchedulePaused = true"} {
		require.NoError(t, validate(predicate, true), predicate)
	}
}

func TestValidateIsolatedVisibilityQuery_ScheduleIDPrecedence(t *testing.T) {
	mapper := chasm.NewTestVisibilitySearchAttributesMapper(map[string]string{sadefs.WorkflowID: sadefs.ScheduleID}, map[string]enumspb.IndexedValueType{sadefs.WorkflowID: enumspb.INDEXED_VALUE_TYPE_KEYWORD})
	for _, custom := range []bool{false, true} {
		namespaceMapper := &searchattribute.TestMapper{WithCustomScheduleID: custom}
		provider := searchattribute.NewTestMapperProvider(namespaceMapper)
		for _, predicate := range []string{"ScheduleId = 'custom-value'", "ScheduleId IN ('custom-value', 'other')", "ScheduleId != 'business-id'"} {
			require.NoError(t, ValidateIsolatedVisibilityQuery(testNamespace, searchattribute.TestNameTypeMap(), provider, mapper, predicate, metrics.NoopMetricsHandler, log.NewNoopLogger()))
		}
		field, _, err := query.ResolveSearchAttributeAlias(sadefs.ScheduleID, testNamespace, namespaceMapper, searchattribute.TestNameTypeMap(), mapper, 0)
		require.NoError(t, err)
		if custom {
			require.Equal(t, searchattribute.TestScheduleIDFieldName, field)
		} else {
			require.Equal(t, sadefs.WorkflowID, field)
		}
	}
	// A mapped alias that resolves to division must also be rejected.
	err := ValidateIsolatedVisibilityQuery(testNamespace, searchattribute.TestNameTypeMap(), searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{}), mapper, "AliasForTemporalNamespaceDivision = 'other'", metrics.NoopMetricsHandler, log.NewNoopLogger())
	require.ErrorContains(t, err, "namespace division")
}
