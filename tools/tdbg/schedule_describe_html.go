package tdbg

import (
	_ "embed"
	"fmt"
	"html/template"
	"io"
)

//go:embed schedule_describe.html.tmpl
var scheduleDescribeHTMLTemplate string

func renderScheduleDescribeHTML(output io.Writer, report *scheduleDescribeReport) error {
	tmpl, err := template.New("schedule-describe").Funcs(template.FuncMap{
		"displayPath": func(path string) string {
			if path == "" {
				return "Scheduler root"
			}
			return path
		},
	}).Parse(scheduleDescribeHTMLTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse schedule HTML template: %w", err)
	}
	if err := tmpl.Execute(output, report); err != nil {
		return fmt.Errorf("failed to render schedule report as HTML: %w", err)
	}
	return nil
}
