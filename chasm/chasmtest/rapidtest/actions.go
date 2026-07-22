package rapidtest

import (
	"fmt"

	"pgregory.net/rapid"
)

// Action is a weighted state-machine operation. A zero weight means one.
type Action struct {
	Name   string
	Weight int
	Run    func(*rapid.T)
}

// ActionMap converts weighted actions into Rapid's native action map.
func ActionMap(actions ...Action) map[string]func(*rapid.T) {
	result := make(map[string]func(*rapid.T))
	for _, action := range actions {
		if action.Run == nil {
			panic(fmt.Sprintf("rapidtest: action %q has no function", action.Name)) //nolint:forbidigo // Invalid static test configuration cannot continue.
		}
		weight := action.Weight
		if weight == 0 {
			weight = 1
		}
		if weight < 0 {
			panic(fmt.Sprintf("rapidtest: action %q has negative weight %d", action.Name, action.Weight)) //nolint:forbidigo // Invalid static test configuration cannot continue.
		}
		if action.Name == "" && weight != 1 {
			panic("rapidtest: invariant action must have weight one") //nolint:forbidigo // Invalid static test configuration cannot continue.
		}
		for copyIndex := 0; copyIndex < weight; copyIndex++ {
			name := action.Name
			if copyIndex > 0 {
				name = fmt.Sprintf("%s#%d", action.Name, copyIndex+1)
			}
			if _, exists := result[name]; exists {
				panic(fmt.Sprintf("rapidtest: duplicate action name %q", name)) //nolint:forbidigo // Invalid static test configuration cannot continue.
			}
			result[name] = action.Run
		}
	}
	return result
}
