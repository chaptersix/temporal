package flakereport

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type codeOwnerRule struct {
	pattern *regexp.Regexp
	owners  []string
}

type codeOwners struct {
	rules []codeOwnerRule
}

func loadCodeOwners(path string) (codeOwners, error) {
	file, err := os.Open(path)
	if err != nil {
		return codeOwners{}, fmt.Errorf("failed to open CODEOWNERS: %w", err)
	}
	defer file.Close()

	var result codeOwners
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pattern, err := compileCodeOwnerPattern(fields[0])
		if err != nil {
			return codeOwners{}, fmt.Errorf("invalid CODEOWNERS pattern %q: %w", fields[0], err)
		}
		result.rules = append(result.rules, codeOwnerRule{pattern: pattern, owners: fields[1:]})
	}
	if err := scanner.Err(); err != nil {
		return codeOwners{}, fmt.Errorf("failed to read CODEOWNERS: %w", err)
	}
	return result, nil
}

func (c codeOwners) owners(path string) []string {
	path = strings.TrimPrefix(filepath.ToSlash(filepath.Clean(path)), "./")
	var owners []string
	for _, rule := range c.rules {
		if rule.pattern.MatchString(path) {
			owners = append([]string(nil), rule.owners...)
		}
	}
	return owners
}

func compileCodeOwnerPattern(pattern string) (*regexp.Regexp, error) {
	anchored := strings.HasPrefix(pattern, "/")
	pattern = strings.TrimPrefix(pattern, "/")
	directory := strings.HasSuffix(pattern, "/")
	pattern = strings.TrimSuffix(pattern, "/")
	hasSlash := strings.Contains(pattern, "/")

	var expression strings.Builder
	if anchored || hasSlash {
		expression.WriteString("^")
	} else {
		expression.WriteString("^(?:.*/)?")
	}
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				expression.WriteString(".*")
				i++
			} else {
				expression.WriteString("[^/]*")
			}
		case '?':
			expression.WriteString("[^/]")
		default:
			expression.WriteString(regexp.QuoteMeta(string(pattern[i])))
		}
	}
	if directory {
		expression.WriteString("(?:/.*)?$")
	} else if !hasSlash && !anchored {
		expression.WriteString("(?:/.*)?$")
	} else {
		expression.WriteString("$")
	}
	return regexp.Compile(expression.String())
}

func validateSourcePath(repoRoot, candidate string) (string, bool) {
	clean := filepath.Clean(filepath.FromSlash(strings.TrimSpace(candidate)))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", false
	}
	info, err := os.Stat(filepath.Join(repoRoot, clean))
	if err != nil || info.IsDir() {
		return "", false
	}
	return filepath.ToSlash(clean), true
}
