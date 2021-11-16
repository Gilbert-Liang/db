package query

import (
	"db/parser/cnosql"
	"errors"
	"regexp"
)

// RewriteStatement rewrites stmt into a new statement, if applicable.
func RewriteStatement(stmt cnosql.Statement) (cnosql.Statement, error) {
	switch stmt := stmt.(type) {
	case *cnosql.ShowFieldKeysStatement:
		return rewriteShowFieldKeysStatement(stmt)
	case *cnosql.ShowMetricsStatement:
		return rewriteShowMetricsStatement(stmt)
	case *cnosql.ShowSeriesStatement:
		return rewriteShowSeriesStatement(stmt)
	case *cnosql.ShowTagKeysStatement:
		return rewriteShowTagKeysStatement(stmt)
	case *cnosql.ShowTagValuesStatement:
		return rewriteShowTagValuesStatement(stmt)
	default:
		return stmt, nil
	}
}

func rewriteShowFieldKeysStatement(stmt *cnosql.ShowFieldKeysStatement) (cnosql.Statement, error) {
	return &cnosql.SelectStatement{
		Fields: cnosql.Fields([]*cnosql.Field{
			{Expr: &cnosql.VarRef{Val: "fieldKey"}},
			{Expr: &cnosql.VarRef{Val: "fieldType"}},
		}),
		Sources:    rewriteSources(stmt.Sources, "_fieldKeys", stmt.Database),
		Condition:  rewriteSourcesCondition(stmt.Sources, nil),
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		Dedupe:     true,
		IsRawQuery: true,
	}, nil
}

func rewriteShowMetricsStatement(stmt *cnosql.ShowMetricsStatement) (cnosql.Statement, error) {
	var sources cnosql.Sources
	if stmt.Source != nil {
		sources = cnosql.Sources{stmt.Source}
	}

	// Currently time based SHOW METRIC queries can't be supported because
	// it's not possible to appropriate set operations such as a negated regex
	// using the query engine.
	if cnosql.HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW METRICS doesn't support time in WHERE clause")
	}

	// rewrite condition to push a source metric into a "_name" tag.
	stmt.Condition = rewriteSourcesCondition(sources, stmt.Condition)
	return stmt, nil
}

func rewriteShowSeriesStatement(stmt *cnosql.ShowSeriesStatement) (cnosql.Statement, error) {
	s := &cnosql.SelectStatement{
		Condition:  stmt.Condition,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		StripName:  true,
		Dedupe:     true,
		IsRawQuery: true,
	}
	// Check if we can exclusively use the index.
	if !cnosql.HasTimeExpr(stmt.Condition) {
		s.Fields = []*cnosql.Field{{Expr: &cnosql.VarRef{Val: "key"}}}
		s.Sources = rewriteSources(stmt.Sources, "_series", stmt.Database)
		s.Condition = rewriteSourcesCondition(s.Sources, s.Condition)
		return s, nil
	}

	// The query is bounded by time then it will have to query TSM data rather
	// than utilising the index via system iterators.
	s.Fields = []*cnosql.Field{
		{Expr: &cnosql.VarRef{Val: "_seriesKey"}, Alias: "key"},
	}
	s.Sources = rewriteSources2(stmt.Sources, stmt.Database)
	return s, nil
}

func rewriteShowTagValuesStatement(stmt *cnosql.ShowTagValuesStatement) (cnosql.Statement, error) {
	var expr cnosql.Expr
	if list, ok := stmt.TagKeyExpr.(*cnosql.ListLiteral); ok {
		for _, tagKey := range list.Vals {
			tagExpr := &cnosql.BinaryExpr{
				Op:  cnosql.EQ,
				LHS: &cnosql.VarRef{Val: "_tagKey"},
				RHS: &cnosql.StringLiteral{Val: tagKey},
			}

			if expr != nil {
				expr = &cnosql.BinaryExpr{
					Op:  cnosql.OR,
					LHS: expr,
					RHS: tagExpr,
				}
			} else {
				expr = tagExpr
			}
		}
	} else {
		expr = &cnosql.BinaryExpr{
			Op:  stmt.Op,
			LHS: &cnosql.VarRef{Val: "_tagKey"},
			RHS: stmt.TagKeyExpr,
		}
	}

	// Set condition or "AND" together.
	condition := stmt.Condition
	if condition == nil {
		condition = expr
	} else {
		condition = &cnosql.BinaryExpr{
			Op:  cnosql.AND,
			LHS: &cnosql.ParenExpr{Expr: condition},
			RHS: &cnosql.ParenExpr{Expr: expr},
		}
	}
	condition = rewriteSourcesCondition(stmt.Sources, condition)

	return &cnosql.ShowTagValuesStatement{
		Database:   stmt.Database,
		Op:         stmt.Op,
		TagKeyExpr: stmt.TagKeyExpr,
		Condition:  condition,
		SortFields: stmt.SortFields,
		Limit:      stmt.Limit,
		Offset:     stmt.Offset,
	}, nil
}

func rewriteShowTagKeysStatement(stmt *cnosql.ShowTagKeysStatement) (cnosql.Statement, error) {
	return &cnosql.ShowTagKeysStatement{
		Database:   stmt.Database,
		Condition:  rewriteSourcesCondition(stmt.Sources, stmt.Condition),
		SortFields: stmt.SortFields,
		Limit:      stmt.Limit,
		Offset:     stmt.Offset,
		SLimit:     stmt.SLimit,
		SOffset:    stmt.SOffset,
	}, nil
}

// rewriteSources rewrites sources to include the provided system iterator.
//
// rewriteSources also sets the default database where necessary.
func rewriteSources(sources cnosql.Sources, systemIterator, defaultDatabase string) cnosql.Sources {
	newSources := cnosql.Sources{}
	for _, src := range sources {
		if src == nil {
			continue
		}
		mm := src.(*cnosql.Metric)
		database := mm.Database
		if database == "" {
			database = defaultDatabase
		}

		newM := mm.Clone()
		newM.SystemIterator, newM.Database = systemIterator, database
		newSources = append(newSources, newM)
	}

	if len(newSources) <= 0 {
		return append(newSources, &cnosql.Metric{
			Database:       defaultDatabase,
			SystemIterator: systemIterator,
		})
	}
	return newSources
}

// rewriteSourcesCondition rewrites sources into `name` expressions.
// Merges with cond and returns a new condition.
func rewriteSourcesCondition(sources cnosql.Sources, cond cnosql.Expr) cnosql.Expr {
	if len(sources) == 0 {
		return cond
	}

	// Generate an OR'd set of filters on source name.
	var scond cnosql.Expr
	for _, source := range sources {
		mm := source.(*cnosql.Metric)

		// Generate a filtering expression on the metric name.
		var expr cnosql.Expr
		if mm.Regex != nil {
			expr = &cnosql.BinaryExpr{
				Op:  cnosql.EQREGEX,
				LHS: &cnosql.VarRef{Val: "_name"},
				RHS: &cnosql.RegexLiteral{Val: mm.Regex.Val},
			}
		} else if mm.Name != "" {
			expr = &cnosql.BinaryExpr{
				Op:  cnosql.EQ,
				LHS: &cnosql.VarRef{Val: "_name"},
				RHS: &cnosql.StringLiteral{Val: mm.Name},
			}
		}

		if scond == nil {
			scond = expr
		} else {
			scond = &cnosql.BinaryExpr{
				Op:  cnosql.OR,
				LHS: scond,
				RHS: expr,
			}
		}
	}

	// This is the case where the original query has a WHERE on a tag, and also
	// is requesting from a specific source.
	if cond != nil && scond != nil {
		return &cnosql.BinaryExpr{
			Op:  cnosql.AND,
			LHS: &cnosql.ParenExpr{Expr: scond},
			RHS: &cnosql.ParenExpr{Expr: cond},
		}
	} else if cond != nil {
		// This is the case where the original query has a WHERE on a tag but
		// is not requesting from a specific source.
		return cond
	}
	return scond
}

func rewriteSources2(sources cnosql.Sources, database string) cnosql.Sources {
	if len(sources) == 0 {
		sources = cnosql.Sources{&cnosql.Metric{Regex: &cnosql.RegexLiteral{Val: matchAllRegex.Copy()}}}
	}
	for _, source := range sources {
		switch source := source.(type) {
		case *cnosql.Metric:
			if source.Database == "" {
				source.Database = database
			}
		}
	}
	return sources
}

var matchAllRegex = regexp.MustCompile(`.+`)
