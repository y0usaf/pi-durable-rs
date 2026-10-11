package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/MichaelKinsy/PiG/ai"
)

func main() {
	var out []map[string]any
	for _, provider := range ai.ListProviders() {
		for _, generated := range ai.ListModels(provider) {
			model := generated.ToModel()
			caps := model.Capabilities
			levels := map[string]any{}
			for level, value := range model.ThinkingLevelMap {
				if value == nil {
					levels[string(level)] = nil
				} else {
					levels[string(level)] = *value
				}
			}
			tiers := []map[string]any{}
			for _, tier := range caps.CostTiers {
				tiers = append(tiers, map[string]any{"inputTokensAbove": tier.InputTokensAbove, "input": tier.InputCostPer1M, "output": tier.OutputCostPer1M, "cacheRead": tier.CacheReadCostPer1M, "cacheWrite": tier.CacheWriteCostPer1M})
			}
			record := map[string]any{
				"id":            model.ID,
				"name":          model.DisplayName,
				"api":           string(model.ProviderMeta.API),
				"provider":      model.ProviderMeta.ProviderID,
				"reasoning":     model.ProviderMeta.Reasoning,
				"contextWindow": caps.ContextWindow,
				"maxTokens":     caps.MaxOutputTokens,
				"input":         model.Input,
				"cost": map[string]any{"input": caps.InputCostPer1M, "output": caps.OutputCostPer1M, "cacheRead": caps.CacheReadCostPer1M, "cacheWrite": caps.CacheWriteCostPer1M, "tiers": tiers},
			}
			if model.ProviderMeta.BaseURL != "" {
				record["baseUrl"] = model.ProviderMeta.BaseURL
			}
			if len(model.ProviderMeta.Headers) > 0 {
				record["headers"] = model.ProviderMeta.Headers
			}
			if len(levels) > 0 {
				record["thinkingLevelMap"] = levels
			}
			if model.PromptCache != nil {
				record["promptCache"] = model.PromptCache
			}
			if model.ProviderMeta.Compat != nil {
				record["compat"] = model.ProviderMeta.Compat
			}
			if len(model.SamplingParams) > 0 {
				record["samplingParams"] = model.SamplingParams
			}
			out = append(out, record)
		}
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Stdout.Write(append(data, '\n'))
}
