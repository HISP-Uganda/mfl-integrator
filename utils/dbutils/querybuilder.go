package dbutils

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/HISP-Uganda/mfl-integrator/utils"
)

// Table is a representation of a table in a query
type Table struct {
	Name  string
	Alias string
	// Fields []string
}

// Field is a representation of a table field
type Field struct {
	Name        string
	TablePrefix string // helpful in a join t.name  ...
	Alias       string // if blank name is taken as is
}

// Condition is the representation of a condition in a where clause
type Condition struct {
	Field    Field
	Operator string
	Value    string
}

// Join represents a JOIN in a query
type Join struct {
	Kind  string // INNER, LEFT INNER, LEFT OUTER, JOIN
	Table Table
	On    string
}

// Order represnts a single part of order by clause in the query
type Order struct {
	Field       Field
	Arrangement string
}

// QueryBuilder represnts different parts of an SQL query
type QueryBuilder struct {
	QueryTemplate string // should be SELECT ...., %(fields)s FROM %(table)s
	Table         Table
	Fields        []Field
	Joins         []Join
	Conditions    []Condition
	OrderBy       []Order
	Limit         int64
	Offset        int64
}

// FieldsToString returns the list of fields as they appear in a query
func FieldsToString(fields []Field) string {
	var fieldsStr bytes.Buffer
	for idx, f := range fields {

		switch f.TablePrefix {
		case "":
			fmt.Fprintf(&fieldsStr, "%s %s", f.Name, f.Alias)
			// fieldsStr.WriteString("%s")
		default:
			fmt.Fprintf(&fieldsStr, "%s.%s %s", f.TablePrefix, f.Name, f.Alias)
		}
		if idx != len(fields)-1 {
			fieldsStr.WriteString(", ")
		} else {
			fieldsStr.WriteString(" ")
		}
	}
	return fieldsStr.String()
}

// OrderByToString returns SQL ORDER BY clause for the field:asc|desc properties
func OrderByToString(orders []Order) string {
	var orderByStr bytes.Buffer
	for idx, o := range orders {
		if len(o.Field.TablePrefix) > 0 {
			fmt.Fprintf(&orderByStr, " %s.%s %s ", o.Field.TablePrefix, o.Field.Name, o.Arrangement)
		} else {
			fmt.Fprintf(&orderByStr, " %s %s ", o.Field.Name, o.Arrangement)
		}
		if idx != len(orders)-1 {
			orderByStr.WriteString(", ")
		} else {
			orderByStr.WriteString(" ")
		}
	}

	return orderByStr.String()
}

// QueryConditions return the conditions as they appear in the WHERE clause
func QueryConditions(conditions []Condition) string {
	var condStr bytes.Buffer

	for idx, c := range conditions {

		switch c.Field.TablePrefix {
		case "":
			fmt.Fprintf(&condStr, "%s %s '%s'",
				c.Field.Name, c.Operator, c.Value)
		default:
			fmt.Fprintf(&condStr, "%s.%s %s '%s'",
				c.Field.TablePrefix, c.Field.Name, c.Operator, c.Value)
		}
		if idx != len(conditions)-1 {
			fmt.Fprintf(&condStr, `
	AND `)

		} else {
			condStr.WriteString("")
		}
	}
	return condStr.String()
}

// QueryJoins returns the joins that are part of our query in the QueryBuilder object
func QueryJoins(joins []Join) string {
	var joinStr bytes.Buffer

	for _, j := range joins {
		fmt.Fprintf(&joinStr, `%s JOIN %s %s ON(%s)
`, j.Kind, j.Table.Name, j.Table.Alias, j.On)
	}
	return joinStr.String()
}

// ToSQL return the SQL representation of our QueryBuilder struct
func (q *QueryBuilder) ToSQL(paging bool) string {
	if len(q.Fields) > 0 && len(q.QueryTemplate) > 0 {
		query := fmt.Sprintf(q.QueryTemplate, FieldsToString(q.Fields),
			q.Table.Name+" "+q.Table.Alias, QueryJoins(q.Joins))
		if len(q.Conditions) > 0 {
			var ret string
			if len(q.OrderBy) > 0 {
				ret += fmt.Sprintf(query+"WHERE %s ORDER BY %s %s",
					QueryConditions(q.Conditions), OrderByToString(q.OrderBy),
					q.QueryLimitClause(paging))
			} else {
				ret += fmt.Sprintf(query+"WHERE %s %s",
					QueryConditions(q.Conditions), q.QueryLimitClause(paging))
			}
			return ret
		}
		if len(q.OrderBy) > 0 {
			return fmt.Sprintf(query+" ORDER BY %s %s", OrderByToString(q.OrderBy),
				q.QueryLimitClause(paging))
		}
		return fmt.Sprintf(query+" %s ", q.QueryLimitClause(paging))
	}
	return ""
}

// QueryFiltersToConditions returns a list of conditions with field, operator and value
func QueryFiltersToConditions(filters []string, tableAlias string) []Condition {
	conditions := []Condition{}
	for _, f := range filters {
		cond := strings.Split(f, ":")
		if len(cond) == 3 {
			var op string
			switch strings.ToUpper(cond[1]) {
			case "EQ":
				op = "="
			case "GT":
				op = ">"
			case "LT":
				op = "<="
			case "GE":
				op = ">="
			case "LE":
				op = "<="
			default:
				op = "="
			}
			condition := Condition{
				Field{cond[0], tableAlias, ""}, op, cond[2]}
			conditions = append(conditions, condition)
		}
	}
	return conditions
}

// OrderListToOrderBy returns a list of Order objects to add to an sql order by clause
func OrderListToOrderBy(order []string, tableFields []string, tableAlias string) []Order {
	orderBys := []Order{}
	for _, o := range order {
		oby := strings.Split(o, ":")
		if len(oby) == 2 {
			if utils.SliceContains(tableFields, oby[0]) {
				switch strings.ToLower(oby[1]) {
				case "asc":
					orderBys = append(orderBys, Order{
						Field{oby[0], tableAlias, ""},
						oby[1]})
				case "desc":
					orderBys = append(orderBys, Order{
						Field{oby[0], tableAlias, ""},
						oby[1]})
				}

			}
		}
	}
	return orderBys
}

//QueryLimitClause returns the sql string for the LIMIT clause
func (q *QueryBuilder) QueryLimitClause(paging bool) string {
	if !paging {
		return ""
	}
	return fmt.Sprintf(" LIMIT %d OFFSET %d ", q.Limit, q.Offset)
}
